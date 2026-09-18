"""SQLite 连接与 schema 初始化。

设计要点：
- WAL 模式：读不阻塞写，写之间通过 BEGIN IMMEDIATE 串行化；
- busy_timeout：并发写事务排队等待，避免 "database is locked" 直接失败；
- isolation_level=None（自动提交），事务边界由代码显式控制。
"""
from __future__ import annotations

import json
import os
import sqlite3

from . import config, tree

SCHEMA = """
CREATE TABLE IF NOT EXISTS buckets (
    key             TEXT PRIMARY KEY,
    limit_amount    INTEGER NOT NULL CHECK (limit_amount > 0),
    window_seconds  REAL    NOT NULL CHECK (window_seconds > 0),
    parent_key      TEXT REFERENCES buckets (key)
);

-- 滚动窗口用量事件：每次放行按"贡献链"写入——叶桶一条直接事件
-- (is_direct=1)，其全部祖先各一条派生事件 (is_direct=0)；
-- 同一请求共享祖先的多个后代只汇总为一条派生事件
CREATE TABLE IF NOT EXISTS usage_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_key  TEXT    NOT NULL REFERENCES buckets (key),
    request_id  TEXT    NOT NULL,
    amount      INTEGER NOT NULL CHECK (amount > 0),
    created_at  REAL    NOT NULL,
    is_direct   INTEGER NOT NULL DEFAULT 1 CHECK (is_direct IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_usage_events_bucket_ts
    ON usage_events (bucket_key, created_at);

-- 幂等表：request_id 首次出现时固化载荷哈希与判定结果
-- release_id 非空表示该消费已被对应释放单成功释放
CREATE TABLE IF NOT EXISTS requests (
    request_id   TEXT PRIMARY KEY,
    payload_hash TEXT    NOT NULL,
    payload      TEXT    NOT NULL,
    decision     TEXT    NOT NULL,
    allowed      INTEGER NOT NULL,
    release_id   TEXT,
    created_at   REAL    NOT NULL
);

-- 释放幂等表：release_id 首次成功释放时固化目标 request_id 与释放结果
CREATE TABLE IF NOT EXISTS releases (
    release_id  TEXT PRIMARY KEY,
    request_id  TEXT    NOT NULL,
    result      TEXT    NOT NULL,
    created_at  REAL    NOT NULL
);

-- 调整幂等表：adjustment_id 首次成功调整时固化完整载荷哈希与调整结果。
-- 调整语义为"替换"：成功后该请求对各桶的用量贡献以 payload 中的
-- 目标桶列表为准（原贡献事件已在同事务中被原子删除替换）。
CREATE TABLE IF NOT EXISTS adjustments (
    adjustment_id TEXT PRIMARY KEY,
    request_id    TEXT    NOT NULL,
    payload_hash  TEXT    NOT NULL,
    payload       TEXT    NOT NULL,
    result        TEXT    NOT NULL,
    created_at    REAL    NOT NULL
);

-- 哈希审计链：仅追加。seq 全局递增，首条 prev_hash 为 64 个 0，
-- hash = SHA-256(prev_hash 的原始32字节 + canonical JSON 事件)
CREATE TABLE IF NOT EXISTS audit_log (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    prev_hash    TEXT    NOT NULL,
    hash         TEXT    NOT NULL,
    op           TEXT    NOT NULL,
    business_id  TEXT    NOT NULL,
    result       TEXT    NOT NULL,
    event_json   TEXT    NOT NULL,
    occurred_at  REAL    NOT NULL
);
"""

# 旧库增量迁移：
# - requests 表补充 release_id 列；
# - buckets 表补充 parent_key 列（多级配额树）；
# - usage_events 表补充 is_direct 列（历史事件均视为直接贡献）
_MIGRATIONS = (
    (
        "requests",
        "release_id",
        "ALTER TABLE requests ADD COLUMN release_id TEXT",
    ),
    (
        "buckets",
        "parent_key",
        "ALTER TABLE buckets ADD COLUMN parent_key TEXT REFERENCES buckets (key)",
    ),
    (
        "usage_events",
        "is_direct",
        "ALTER TABLE usage_events ADD COLUMN is_direct INTEGER NOT NULL "
        "DEFAULT 1 CHECK (is_direct IN (0, 1))",
    ),
)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or config.DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={config.BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str | None = None) -> None:
    path = db_path or config.DB_PATH
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        _apply_migrations(conn)
    finally:
        conn.close()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """对旧版本数据库执行增量列迁移（SQLite 无 ADD COLUMN IF NOT EXISTS）。"""
    for table, column, ddl in _MIGRATIONS:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(ddl)


def seed_buckets(config_path: str, db_path: str | None = None) -> list[str]:
    """从 JSON 文件播种桶配置；同名桶按文件内容更新（配置文件即事实源）。

    条目可携带可选 ``parent_key`` 形成多级配额树；播种在单事务内完成：
    合并存量父级后校验缺失父桶/自引用/环，任一非法整体回滚并抛出
    ValueError；同批条目按父先子后排序写入以满足外键。
    """
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    entries = raw["buckets"] if isinstance(raw, dict) else raw
    entries = [
        {
            "key": e["key"],
            "limit_amount": int(e["limit"]),
            "window_seconds": float(e["window_seconds"]),
            "parent_key": e.get("parent_key"),
        }
        for e in entries
    ]

    conn = connect(db_path)
    seeded: list[str] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        parents = {
            r["key"]: r["parent_key"]
            for r in conn.execute("SELECT key, parent_key FROM buckets")
        }
        for e in entries:
            parents[e["key"]] = e["parent_key"]

        bad_key = tree.validate_forest(parents)
        if bad_key is not None:
            bad_parent = parents[bad_key]
            if bad_parent is not None and bad_parent not in parents:
                raise ValueError(
                    f"桶 '{bad_key}' 的 parent_key '{bad_parent}' 未配置"
                )
            raise ValueError(
                f"桶 '{bad_key}' 的 parent_key 配置形成自引用或环"
            )

        for e in tree.topo_order_seed(entries):
            conn.execute(
                """
                INSERT INTO buckets (key, limit_amount, window_seconds, parent_key)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (key) DO UPDATE SET
                    limit_amount   = excluded.limit_amount,
                    window_seconds = excluded.window_seconds,
                    parent_key     = excluded.parent_key
                """,
                (e["key"], e["limit_amount"], e["window_seconds"], e["parent_key"]),
            )
            seeded.append(e["key"])
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()
    return seeded


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
