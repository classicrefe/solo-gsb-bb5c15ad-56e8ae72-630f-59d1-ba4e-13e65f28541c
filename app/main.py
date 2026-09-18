"""FastAPI 入口：多桶滚动配额判定 API。

核心保证：
- 同一请求的所有桶在单个 BEGIN IMMEDIATE 事务内"检查 + 扣减"，
  任一超限则整体不扣减（原子性）；
- BEGIN IMMEDIATE 使写事务串行化，并发请求不会突破限额；
- request_id 首次出现时固化载荷与判定，重复提交原样返回，
  载荷变化返回 409；
- 首次判定（放行/拒绝）与首次成功释放在各自原事务内追加一条
  可校验哈希审计链记录（见 app/audit.py），幂等重放不重复记录；
- POST /v1/quota/adjustments 在单个 BEGIN IMMEDIATE 事务内先排除
  已放行且未释放请求的原贡献、再按当前桶配置校验目标列表，任一未知
  桶或超限则原贡献不变，全部通过才原子替换全部用量事件；首次成功
  调整在原事务追加一条审计记录，同 adjustment_id 同载荷重放原样
  返回、改换载荷返回 409；
- 释放撤销的是该请求当前（可能已被调整）的用量贡献。

桶可配置可选 ``parent_key`` 形成多级配额树：消费叶桶时用量同时计入其
全部祖先并原子校验每一层限额（任一层超限整体不扣减）；同一请求中共享
祖先的多个桶，祖先只累计各后代金额之和；调整/释放按同一展开规则替换、
撤销整条贡献链。
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import audit, config, simulation, tree
from .db import _rollback_quietly, connect, init_db, seed_buckets
from .quota import earliest_retry_at, merge_items, payload_hash
from .schemas import (
    AdjustmentRequest,
    BucketConfig,
    ConsumeRequest,
    ReleaseRequest,
    SimulationRequest,
)

logger = logging.getLogger("quota")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if config.BUCKETS_CONFIG and os.path.exists(config.BUCKETS_CONFIG):
        seeded = seed_buckets(config.BUCKETS_CONFIG)
        logger.info("seeded buckets from %s: %s", config.BUCKETS_CONFIG, seeded)
    yield


app = FastAPI(title="Rolling Quota API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------- 配额判定

@app.post("/v1/quota/consume")
def consume(req: ConsumeRequest):
    """检查并扣减一组桶；任一超限则全部不扣减。

    - 200：全部放行，已扣减；
    - 429：至少一个桶超限，返回受限桶与最早重试时间，未做任何扣减；
    - 409：request_id 已存在但载荷不同；
    - 404：存在未配置的桶。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    phash = payload_hash(items)
    payload_json = json.dumps(
        {"request_id": req.request_id, "buckets": items}, sort_keys=True
    )

    replayed = False
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT payload_hash, decision FROM requests WHERE request_id = ?",
                (req.request_id,),
            ).fetchone()

            if row is not None:
                # 幂等：同 request_id 直接返回已固化的判定，不再扣减
                if row["payload_hash"] != phash:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"request_id '{req.request_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 request_id"
                        ),
                    )
                decision = json.loads(row["decision"])
                replayed = True
                conn.execute("COMMIT")
            else:
                decision = _evaluate_and_consume(conn, req.request_id, items, now)
                conn.execute(
                    "INSERT INTO requests "
                    "(request_id, payload_hash, payload, decision, allowed, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        req.request_id,
                        phash,
                        payload_json,
                        json.dumps(decision),
                        1 if decision["allowed"] else 0,
                        now,
                    ),
                )
                # 首次判定（放行或拒绝）在同一事务内追加审计记录；
                # 幂等重放走上面的已存在分支，不会重复记录
                audit.append_event(
                    conn,
                    op=audit.OP_CONSUME,
                    business_id=req.request_id,
                    result=(
                        audit.RESULT_ALLOWED
                        if decision["allowed"]
                        else audit.RESULT_DENIED
                    ),
                    occurred_at=now,
                )
                conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return _decision_response(decision, replayed)


def _load_bucket_map(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    """读取全部桶配置（含 parent_key），供祖先链展开使用。"""
    return {
        r["key"]: r
        for r in conn.execute(
            "SELECT key, limit_amount, window_seconds, parent_key FROM buckets"
        )
    }


def _window_events(
    conn: sqlite3.Connection, key: str, window: float, now: float
) -> list[tuple[float, int]]:
    """桶当前滚动窗口内 (created_at, amount) 事件（直接 + 派生合计即总用量）。"""
    return [
        (r["created_at"], r["amount"])
        for r in conn.execute(
            "SELECT created_at, amount FROM usage_events "
            "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
            (key, now - window),
        )
    ]


def _check_chain(
    conn: sqlite3.Connection,
    chain: list[dict],
    bucket_map: dict[str, sqlite3.Row],
    now: float,
) -> list[dict]:
    """对贡献链涉及的每一层逐桶做滚动窗口容量校验，返回受限桶列表。

    chain 中同一桶至多两条（is_direct 直接 / 派生汇总），新增占用为
    各条 amount 之和；窗口用量为现存事件总和（直接 + 派生），故
    direct_used / total_used 在每一层都被原子校验。
    """
    # 合并同桶新增占用（保留直接/派生明细供用量分解）
    add_by_key: dict[str, int] = {}
    for c in chain:
        add_by_key[c["key"]] = add_by_key.get(c["key"], 0) + c["amount"]

    limited: list[dict] = []
    for key in sorted(add_by_key):
        bucket = bucket_map[key]
        limit = bucket["limit_amount"]
        window = bucket["window_seconds"]
        events = _window_events(conn, key, window, now)
        used = sum(a for _, a in events)
        requested = add_by_key[key]

        if used + requested > limit:
            retry_at = earliest_retry_at(events, limit, window, requested, now)
            limited.append(
                {
                    "key": key,
                    "limit": limit,
                    "window_seconds": window,
                    "used": used,
                    "requested": requested,
                    "retry_at": retry_at,
                    "retry_after_seconds": (
                        None if retry_at is None else max(0.0, retry_at - now)
                    ),
                }
            )
    return limited


def _limited_decision(
    *,
    limited: list[dict],
    now: float,
    request_id: str | None = None,
    adjustment_id: str | None = None,
    extra: dict | None = None,
) -> dict:
    """构造受限（429）判定体：多桶受限时整体重试时间取各桶最大值。"""
    retry_times = [b["retry_at"] for b in limited]
    overall_retry_at = (
        max(retry_times) if all(t is not None for t in retry_times) else None
    )
    body: dict = {
        "allowed": False,
        "limited_buckets": limited,
        "retry_at": overall_retry_at,
        "retry_after_seconds": (
            None if overall_retry_at is None
            else max(0.0, overall_retry_at - now)
        ),
        "decided_at": now,
    }
    if request_id is not None:
        body["request_id"] = request_id
    if adjustment_id is not None:
        # 调整的 429 响应以 adjusted 字段标识（与 200 调整响应对齐）
        body["adjusted"] = False
        body["adjustment_id"] = adjustment_id
    if extra:
        body.update(extra)
    return body


def _insert_chain_events(
    conn: sqlite3.Connection,
    chain: list[dict],
    request_id: str,
    created_at: float,
) -> None:
    """按贡献链写入用量事件（每桶至多直接/派生各一条）。"""
    for c in chain:
        conn.execute(
            "INSERT INTO usage_events "
            "(bucket_key, request_id, amount, created_at, is_direct) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                c["key"],
                request_id,
                c["amount"],
                created_at,
                1 if c["is_direct"] else 0,
            ),
        )


def _request_contribution_chain(
    conn: sqlite3.Connection, request_id: str
) -> list[dict]:
    """汇总请求现存用量事件为规范化贡献链（同桶直接/派生金额合并）。"""
    return merge_items(
        [
            {"key": r["bucket_key"], "amount": r["amount"]}
            for r in conn.execute(
                "SELECT bucket_key, amount FROM usage_events WHERE request_id = ?",
                (request_id,),
            ).fetchall()
        ]
    )


def _evaluate_and_consume(
    conn: sqlite3.Connection, request_id: str, items: list[dict], now: float
) -> dict:
    """在事务内检查贡献链全部层级；全部通过才写入整条扣减事件。"""
    bucket_map = _load_bucket_map(conn)
    missing = [it["key"] for it in items if it["key"] not in bucket_map]
    if missing:
        key = missing[0]
        raise HTTPException(
            status_code=404,
            detail=f"桶 '{key}' 未配置，请先通过 PUT /v1/buckets/{key} 创建",
        )

    parents = {k: b["parent_key"] for k, b in bucket_map.items()}
    chain = tree.expand_contributions(items, parents)
    limited = _check_chain(conn, chain, bucket_map, now)

    if limited:
        # 任一层超限：整条链都不扣减
        return _limited_decision(request_id=request_id, limited=limited, now=now)

    # 全部通过：同事务写入叶桶直接事件与全部祖先派生事件
    _insert_chain_events(conn, chain, request_id, now)
    return {
        "request_id": request_id,
        "allowed": True,
        "granted": items,
        "decided_at": now,
    }


def _decision_response(decision: dict, replayed: bool) -> JSONResponse:
    headers = {}
    if replayed:
        headers["X-Idempotent-Replay"] = "true"
    if decision["allowed"]:
        return JSONResponse(status_code=200, content=decision, headers=headers)
    if decision.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(decision["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=decision, headers=headers)


# ---------------------------------------------------------------- 配额释放

@app.post("/v1/quota/releases")
def release(req: ReleaseRequest):
    """撤销一次已放行消费对所有桶的用量贡献。

    首次释放在单个 BEGIN IMMEDIATE 事务内完成"校验 → 删除该请求全部用量
    事件 → 标记消费已释放 → 固化释放结果"，任一环节失败整体回滚，
    不会出现部分桶已恢复。

    - 200：释放成功（或同 release_id + 同 request_id 重放，原样返回并带
      X-Idempotent-Replay: true）；
    - 404：原消费 request_id 不存在；
    - 409：原判定为拒绝 / 已被其他 release_id 释放 / release_id 已指向
      其他 request_id / 并发竞争落败。
    """
    now = time.time()

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT request_id, result FROM releases WHERE release_id = ?",
                (req.release_id,),
            ).fetchone()
            if existing is not None:
                # 幂等：同 release_id 必须指向同一目标，原样返回固化结果
                if existing["request_id"] != req.request_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"release_id '{req.release_id}' 已用于释放 "
                            f"request_id '{existing['request_id']}'，不可改指其他消费请求"
                        ),
                    )
                conn.execute("COMMIT")
                result = json.loads(existing["result"])
                replayed = True
            else:
                result = _release_consumption(conn, req.release_id, req.request_id, now)
                conn.execute(
                    "INSERT INTO releases (release_id, request_id, result, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        req.release_id,
                        req.request_id,
                        json.dumps(result),
                        now,
                    ),
                )
                # 仅首次成功释放在同一事务内追加审计记录；
                # 失败（404/409）整体回滚、重放走已存在分支，均不记录
                audit.append_event(
                    conn,
                    op=audit.OP_RELEASE,
                    business_id=req.release_id,
                    result=audit.RESULT_RELEASED,
                    occurred_at=now,
                )
                conn.execute("COMMIT")
                replayed = False
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _release_consumption(
    conn: sqlite3.Connection, release_id: str, request_id: str, now: float
) -> dict:
    """在事务内撤销一个消费请求对所有桶的用量贡献并记录占用关系。"""
    row = conn.execute(
        "SELECT allowed, release_id FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"消费请求 '{request_id}' 不存在，无法释放",
        )
    if not row["allowed"]:
        # 原判定为拒绝：从未产生任何扣减，无可释放内容
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 原判定为拒绝（429），未发生扣减，不可释放",
        )
    if row["release_id"] is not None:
        # 同一消费只能由一个 release_id 成功释放；并发竞争中的其余请求亦落到此
        raise HTTPException(
            status_code=409,
            detail=(
                f"消费请求 '{request_id}' 已被 release_id '{row['release_id']}' 释放；"
                "同一消费请求只能释放一次"
            ),
        )

    # 撤销该请求当前的整条贡献链（叶桶直接事件 + 祖先派生事件）：
    # 经调整后贡献来自替换写入的事件，故以现存事件为准（调整发生在释放
    # 之前，事件与当前贡献一一对应），按桶汇总各直接/派生金额
    current = _request_contribution_chain(conn, request_id)
    deleted = conn.execute(
        "DELETE FROM usage_events WHERE request_id = ?",
        (request_id,),
    ).rowcount
    # 条件 UPDATE 兜底并发竞争：仅当尚未被其他释放单占用时成功
    cur = conn.execute(
        "UPDATE requests SET release_id = ? WHERE request_id = ? AND release_id IS NULL",
        (release_id, request_id),
    )
    if cur.rowcount != 1:  # pragma: no cover - 理论上前置检查已覆盖，防御性保留
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 已被其他释放单抢先释放",
        )

    return {
        "release_id": release_id,
        "request_id": request_id,
        "released": True,
        "released_buckets": current,
        "events_reverted": deleted,
        "released_at": now,
    }


# ---------------------------------------------------------------- 配额调整

@app.post("/v1/quota/adjustments")
def adjust(req: AdjustmentRequest):
    """把一次已放行且未释放消费的桶用量贡献**原子替换**为目标桶列表。

    在单个 ``BEGIN IMMEDIATE`` 事务内：先从窗口用量中排除该请求的原贡献
    （删除其全部用量事件），再按**当前桶配置**对目标列表逐桶做容量校验；
    任一目标桶未知或任一桶（在排除原贡献后）仍超限，则整体回滚、原贡献
    原样保留；全部通过才删除原事件并写入替换事件、固化调整结果，并在
    同一事务追加一条审计记录。

    - 200：调整成功（或同 adjustment_id + 同载荷重放，原样返回并带
      X-Idempotent-Replay: true，不重复写审计）；
    - 404：request_id 不存在 / 存在未配置的目标桶；
    - 409：request_id 原判定为拒绝 / 已释放 / adjustment_id 已用于
      不同载荷（含改指其他 request_id）；
    - 429：目标列表在排除原贡献后仍超限（原贡献不变），含 Retry-After。
    """
    now = time.time()
    items = merge_items([i.model_dump() for i in req.buckets])
    phash = payload_hash(items)
    payload_json = json.dumps(
        {"adjustment_id": req.adjustment_id,
         "request_id": req.request_id, "buckets": items},
        sort_keys=True,
    )

    conn = connect()
    replayed = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT request_id, payload_hash, result FROM adjustments "
                "WHERE adjustment_id = ?",
                (req.adjustment_id,),
            ).fetchone()
            if existing is not None:
                # 幂等：同 adjustment_id 必须同 request_id 且同目标载荷，
                # 原样返回固化结果，不再写库；任一变化返回 409
                if (
                    existing["request_id"] != req.request_id
                    or existing["payload_hash"] != phash
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"adjustment_id '{req.adjustment_id}' 已被不同载荷使用；"
                            "幂等键不可复用，请更换 adjustment_id"
                        ),
                    )
                result = json.loads(existing["result"])
                replayed = True
                conn.execute("COMMIT")
            else:
                result = _evaluate_and_adjust(
                    conn, req.adjustment_id, req.request_id, items, now
                )
                if not result["adjusted"]:
                    # 目标桶超限：不写入替换事件，回滚以保证原贡献不变
                    conn.execute("ROLLBACK")
                else:
                    conn.execute(
                        "INSERT INTO adjustments "
                        "(adjustment_id, request_id, payload_hash, payload, "
                        " result, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            req.adjustment_id,
                            req.request_id,
                            phash,
                            payload_json,
                            json.dumps(result),
                            now,
                        ),
                    )
                    # 首次成功调整在同一事务内追加审计；失败（404/409/429）整体
                    # 回滚、重放走已存在分支，均不记录
                    audit.append_event(
                        conn,
                        op=audit.OP_ADJUST,
                        business_id=req.adjustment_id,
                        result=audit.RESULT_ADJUSTED,
                        occurred_at=now,
                    )
                    conn.execute("COMMIT")
        except Exception:
            _rollback_quietly(conn)
            raise
    finally:
        conn.close()

    if not result["adjusted"]:
        # 目标桶超限：原贡献因回滚保持不变，返回与 consume 同形的 429
        return _adjustment_limited_response(result)

    return JSONResponse(
        status_code=200,
        content=result,
        headers={"X-Idempotent-Replay": "true"} if replayed else {},
    )


def _evaluate_and_adjust(
    conn: sqlite3.Connection,
    adjustment_id: str,
    request_id: str,
    items: list[dict],
    now: float,
) -> dict:
    """事务内排除原贡献链后校验目标链；全部通过才原子替换整条贡献链。"""
    row = conn.execute(
        "SELECT allowed, release_id, created_at FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"消费请求 '{request_id}' 不存在，无法调整",
        )
    if not row["allowed"]:
        raise HTTPException(
            status_code=409,
            detail=f"消费请求 '{request_id}' 原判定为拒绝（429），未发生扣减，不可调整",
        )
    if row["release_id"] is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"消费请求 '{request_id}' 已被 release_id '{row['release_id']}' 释放，"
                "用量贡献链已撤销，不可调整"
            ),
        )

    # 先从窗口用量中排除该请求的原贡献链（当前现存事件；调整为替换语义，
    # 同一 request_id 至多存在一套事件，含直接与全部祖先派生事件）
    previous = _request_contribution_chain(conn, request_id)
    conn.execute("DELETE FROM usage_events WHERE request_id = ?", (request_id,))

    # 再按当前桶配置展开并校验目标贡献链（此刻窗口用量已不含本请求原贡献）
    bucket_map = _load_bucket_map(conn)
    missing = [it["key"] for it in items if it["key"] not in bucket_map]
    if missing:
        key = missing[0]
        raise HTTPException(
            status_code=404,
            detail=(
                f"目标桶 '{key}' 未配置，请先通过 PUT /v1/buckets/{key} 创建"
            ),
        )

    parents = {k: b["parent_key"] for k, b in bucket_map.items()}
    chain = tree.expand_contributions(items, parents)
    limited = _check_chain(conn, chain, bucket_map, now)

    if limited:
        # 任一层超限：不写入替换事件；事务回滚后已删除的原贡献链原样恢复
        return _limited_decision(
            adjustment_id=adjustment_id,
            limited=limited,
            now=now,
            extra={
                "previous_buckets": previous,
                "target_buckets": items,
            },
        )

    # 全部通过：按同一展开规则原子替换整条贡献链。沿用原消费 created_at，
    # 使新贡献与原贡献在同一滚动窗口时刻生效
    created_at = row["created_at"]
    _insert_chain_events(conn, chain, request_id, created_at)
    return {
        "adjustment_id": adjustment_id,
        "request_id": request_id,
        "adjusted": True,
        "previous_buckets": previous,
        "buckets": _request_contribution_chain(conn, request_id),
        "events_replaced": len(chain),
        "adjusted_at": now,
    }


def _adjustment_limited_response(result: dict) -> JSONResponse:
    """目标桶超限：返回与 consume 同形的 429（含 Retry-After）。"""
    headers = {}
    if result.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(math.ceil(result["retry_after_seconds"]))
    return JSONResponse(status_code=429, content=result, headers=headers)


# ---------------------------------------------------------------- 回放模拟

@app.post("/v1/quota/simulations")
def simulate_quota(req: SimulationRequest):
    """在隔离内存中回放历史消费，评估候选桶配置的判定变化。

    给定 epoch 秒区间 ``[from, to)`` 与若干桶的候选
    ``limit/window_seconds``（未覆盖桶沿用当前配置），从 ``from`` 向前
    取各桶生效窗口最大值作为预热期，按 ``(created_at, request_id)``
    稳定顺序读取**首次消费**固化载荷重放滚动过期与多桶全放行/全拒绝。
    仅回放消费、不处理 release；全程只读，不写数据库也不追加审计链。

    - 200：返回实际判定 → 模拟判定的转移计数、每桶模拟峰值与前 100 条
      判定翻转明细；
    - 422：未知桶、候选桶重复、``from >= to`` 或跨度超过 7 天。
    """
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, limit_amount, window_seconds, parent_key FROM buckets"
        ).fetchall()
        db_configs = {
            r["key"]: (r["limit_amount"], float(r["window_seconds"])) for r in rows
        }
        parents = {r["key"]: r["parent_key"] for r in rows}
        overrides = [o.model_dump() for o in req.buckets]
        try:
            override_cfg = simulation.validate_request(
                req.from_, req.to, overrides, set(db_configs)
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        effective = simulation.resolve_configs(db_configs, override_cfg)
        warmup = simulation.warmup_seconds(effective)

        # 预热起点（含）：[from - max_window, to)；只读、不开写事务
        history = conn.execute(
            """
            SELECT created_at, request_id, payload, allowed
              FROM requests
             WHERE created_at >= ? AND created_at < ?
             ORDER BY created_at ASC, request_id ASC
            """,
            (req.from_ - warmup, float(req.to)),
        ).fetchall()
    finally:
        conn.close()

    records = []
    for r in history:
        items = merge_items(json.loads(r["payload"])["buckets"])
        # 历史载荷引用的桶必须仍有生效配置（防御已删除桶的陈旧固化记录）
        missing = [it["key"] for it in items if it["key"] not in effective]
        if missing:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"历史消费 {r['request_id']} 引用了未知/已删除桶 "
                    f"{sorted(missing)}，无法回放"
                ),
            )
        records.append((r["created_at"], r["request_id"], items,
                        bool(r["allowed"])))
    result = simulation.run_simulation(
        records, effective, float(req.from_), parents
    )

    return {
        "from": req.from_,
        "to": req.to,
        "warmup_seconds": warmup,
        "effective_buckets": [
            {"key": k, "limit": lim, "window_seconds": win}
            for k, (lim, win) in sorted(effective.items())
        ],
        "transitions": result["transitions"],
        "bucket_peaks": [
            {"key": k, "simulated_peak": peak}
            for k, peak in result["bucket_peaks"].items()
        ],
        "flip_details": result["flips"],
        "flip_details_truncated": result["flip_truncated"],
        "warmup_records": result["warmup_records"],
        "records_replayed": result["records_replayed"],
    }


# ---------------------------------------------------------------- 哈希审计链

@app.get("/v1/audit")
def get_audit(
    after_seq: int = Query(0, ge=0, description="仅返回 seq 大于该值的记录"),
    limit: int = Query(100, ge=1, le=1000, description="单次返回的最大记录数"),
):
    """按 seq 升序返回审计记录及当前链头。"""
    conn = connect()
    try:
        records, head_seq, head_hash = audit.list_events(conn, after_seq, limit)
    finally:
        conn.close()
    return {
        "records": records,
        "head": {"seq": head_seq, "hash": head_hash},
        "next_after_seq": records[-1]["seq"] if records else after_seq,
    }


@app.get("/v1/audit/verify")
def verify_audit():
    """全链重算并返回校验结果。

    - valid：链是否完整无误；
    - checked_count：校验的记录条数；
    - head_hash：当前链头哈希（空链为 64 个 0）；
    - first_invalid_seq：首个失配记录的 seq，无失配时为 null。
    """
    conn = connect()
    try:
        return audit.verify_chain(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------- 桶管理

@app.put("/v1/buckets/{key}", status_code=200)
def upsert_bucket(key: str, cfg: BucketConfig):
    """创建或更新桶的限额、窗口与可选父级。

    ``parent_key`` 省略（字段不在请求体）时保持既有父级；显式传
    ``null`` 摘除父级；传非空 key 则必须指向已存在桶。更新父级在单个
    ``BEGIN IMMEDIATE`` 事务内拒绝自引用、缺失父桶与任何直接/间接环。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT parent_key FROM buckets WHERE key = ?", (key,)
        ).fetchone()
        parent = cfg.parent_key
        if cfg.model_fields_set.isdisjoint({"parent_key"}) and existing is not None:
            # 省略 parent_key：保持原值（创建场景默认 NULL 根桶）
            parent = existing["parent_key"]

        if parent is not None:
            if parent == key:
                raise HTTPException(
                    status_code=409,
                    detail=f"桶 '{key}' 的 parent_key 不能指向自身",
                )
            parent_row = conn.execute(
                "SELECT 1 FROM buckets WHERE key = ?", (parent,)
            ).fetchone()
            if parent_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"父桶 '{parent}' 未配置，请先通过 "
                        f"PUT /v1/buckets/{parent} 创建"
                    ),
                )
            parents = {
                r["key"]: r["parent_key"]
                for r in conn.execute("SELECT key, parent_key FROM buckets")
            }
            # 在"假设替换"后的父级图上沿链检查：若重新回到 key 即成环
            parents[key] = parent
            if tree.find_cycle(key, parent, parents):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"将桶 '{key}' 的父级设为 '{parent}' 会形成配额环，"
                        "父级链不允许自引用或循环"
                    ),
                )

        conn.execute(
            """
            INSERT INTO buckets (key, limit_amount, window_seconds, parent_key)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                limit_amount   = excluded.limit_amount,
                window_seconds = excluded.window_seconds,
                parent_key     = excluded.parent_key
            """,
            (key, cfg.limit, cfg.window_seconds, parent),
        )
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()
    return {
        "key": key,
        "limit": cfg.limit,
        "window_seconds": cfg.window_seconds,
        "parent_key": parent,
    }


@app.get("/v1/buckets")
def list_buckets():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, limit_amount, window_seconds, parent_key "
            "FROM buckets ORDER BY key"
        ).fetchall()
    finally:
        conn.close()
    return {
        "buckets": [
            {
                "key": r["key"],
                "limit": r["limit_amount"],
                "window_seconds": r["window_seconds"],
                "parent_key": r["parent_key"],
            }
            for r in rows
        ]
    }


@app.get("/v1/buckets/{key}/usage")
def bucket_usage(key: str):
    """查询桶当前滚动窗口用量。

    - direct_used：直接消费本桶的用量（is_direct=1）；
    - total_used：本桶总用量（直接 + 全部后代经祖先链计入的派生用量）；
    - used：保留字段，等同 total_used。
    """
    now = time.time()
    conn = connect()
    try:
        bucket = conn.execute(
            "SELECT limit_amount, window_seconds FROM buckets WHERE key = ?",
            (key,),
        ).fetchone()
        if bucket is None:
            raise HTTPException(status_code=404, detail=f"桶 '{key}' 未配置")
        window = bucket["window_seconds"]
        events = conn.execute(
            "SELECT created_at, amount, is_direct FROM usage_events "
            "WHERE bucket_key = ? AND created_at > ? ORDER BY created_at",
            (key, now - window),
        ).fetchall()
    finally:
        conn.close()

    direct_used = sum(r["amount"] for r in events if r["is_direct"])
    total_used = sum(r["amount"] for r in events)
    oldest = events[0]["created_at"] if events else None
    return {
        "key": key,
        "limit": bucket["limit_amount"],
        "window_seconds": window,
        "used": total_used,
        "direct_used": direct_used,
        "total_used": total_used,
        "remaining": max(0, bucket["limit_amount"] - total_used),
        "window_start": now - window,
        "as_of": now,
        "oldest_event_expires_at": None if oldest is None else oldest + window,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
