"""多级配额树：祖先链展开与父级关系校验。

桶可通过 ``parent_key`` 指向另一个桶形成配额树：消费叶桶时，其用量
同时计入全部祖先，每一层各自维护 ``limit/window_seconds`` 限额。

本模块只提供纯函数（图用普通 dict/list 表示），数据库事务与错误码
转换由调用方负责。
"""
from __future__ import annotations

from typing import Any


def expand_contributions(
    items: list[dict[str, Any]],
    parents: dict[str, str | None],
) -> list[dict[str, Any]]:
    """把直接消费条目展开为整条贡献链（直接条目 + 各祖先的派生条目）。

    :param items: 规范化后的直接消费条目 ``[{"key", "amount"}]``（同一
        桶至多一条，即已按桶合并）；
    :param parents: 全部桶的父级映射 ``{key: parent_key or None}``。
    :returns: 每个被触及桶至多两条条目：
        ``{"key", "amount", "is_direct": True}`` 表示对该桶的直接消费
        （仅当桶本身出现在 items 中），
        ``{"key", "amount", "is_direct": False}`` 表示其全部后代金额之和
        对该桶的派生贡献；共享同一祖先的多个后代在此只汇总为一条，
        不重复生成贡献。返回结果按 (key, is_direct) 稳定排序。
    """
    direct: dict[str, int] = {}
    derived: dict[str, int] = {}

    for it in items:
        key = it["key"]
        amount = it["amount"]
        direct[key] = direct.get(key, 0) + amount

        # 沿父级链上溯；环/缺失父桶由写桶配置时的校验拦截，
        # 这里用 visited 兜底防御陈旧/脏数据，避免死循环
        seen = {key}
        parent = parents.get(key)
        while parent is not None and parent not in seen:
            derived[parent] = derived.get(parent, 0) + amount
            seen.add(parent)
            parent = parents.get(parent)

    chain: list[dict[str, Any]] = []
    for key in sorted(set(direct) | set(derived)):
        if key in direct:
            chain.append({"key": key, "amount": direct[key], "is_direct": True})
        if key in derived:
            chain.append(
                {"key": key, "amount": derived[key], "is_direct": False}
            )
    return chain


def find_cycle(
    key: str,
    proposed_parent: str | None,
    parents: dict[str, str | None],
) -> bool:
    """判断把 ``key`` 的父级改为 ``proposed_parent`` 是否会形成环（含自引用）。

    采用"假设替换后沿父链上溯"：若重新遇到 ``key``，则新边构成环。
    ``parents`` 为当前全量映射（``key`` 的旧父级也在其中）。
    """
    if proposed_parent is None:
        return False
    current: str | None = proposed_parent
    seen: set[str] = set()
    while current is not None:
        if current == key:
            return True
        if current in seen:
            # 存量图本身有环（防御性：每次写桶都校验，正常不可达）；
            # 该环不经过 key，不由本次更新负责
            return False
        seen.add(current)
        current = parents.get(current)
    return False


def validate_forest(parents: dict[str, str | None]) -> str | None:
    """校验整片父级森林是否合法，返回首个问题桶的 key，无问题返回 None。

    依次检查：父桶缺失、自引用、环。返回值供调用方定位问题。
    """
    # 缺失父桶
    for key, parent in parents.items():
        if parent is not None and parent not in parents:
            return key

    # 自引用 / 环：从每个桶沿链上溯，超过桶数必存在环
    for start in parents:
        seen: set[str] = set()
        current: str | None = start
        while current is not None:
            if current in seen:
                return current
            seen.add(current)
            current = parents.get(current)
    return None


def topo_order_seed(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """播种排序：父桶（含父桶也在本批条目中时）先于子桶写入。

    仅用于让同批播种满足外键顺序；条目间若形成环（调用方应已校验），
    无法排序时原样返回。
    """
    keys = {e["key"] for e in entries}
    by_key = {e["key"]: e for e in entries}
    ordered: list[dict[str, Any]] = []
    placed: set[str] = set()

    def visit(k: str, stack: set[str]) -> None:
        if k in placed or k not in by_key:
            return
        if k in stack:  # 环：放弃排序
            return
        stack.add(k)
        parent = by_key[k].get("parent_key")
        if parent in keys:
            visit(parent, stack)
        stack.discard(k)
        if k not in placed:
            placed.add(k)
            ordered.append(by_key[k])

    for e in entries:
        visit(e["key"], set())
    return ordered
