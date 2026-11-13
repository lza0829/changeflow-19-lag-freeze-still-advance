# 07-lag-sla-freeze-gate: lag_sla_breach
"""滞后 SLA 与游标冻结策略的纯领域逻辑。

口径（落页可查，勿与「条数对账」混淆）：
- 滞后 lag = 落地时刻(land_ts) - 事件 event_ts，单位秒。
- 落地时刻取本次分析开始的同一时刻：本批「拟应用」事件共用一个 land_ts，
  因此分布差异完全来自 event_ts，而不是循环处理的墙钟抖动。
- 样本只包含本批拟应用事件（已应用/重复序、缺口隔离事件不进样本）。
- 分位数采用 nearest-rank（与 SQL 常见 PERCENTILE_CONT 口径不同，此处确定性、零依赖）。
- breach 判定：样本中 lag > 阈值的条数 > 0；阈值与策略均可配置保存。
- 策略互斥：freeze_cursor 冻结游标且不落 Sink；ticket_and_advance 记违约票并放行。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

POLICY_FREEZE = "freeze_cursor"
POLICY_TICKET = "ticket_and_advance"
POLICIES = (POLICY_FREEZE, POLICY_TICKET)

DEFAULT_THRESHOLD_SECONDS = 600
DEFAULT_POLICY = POLICY_FREEZE

SETTING_THRESHOLD = "lag_sla_threshold_seconds"
SETTING_POLICY = "lag_sla_policy"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def parse_event_ts(value: str) -> datetime:
    """解析夹具里的 ISO8601 时间（结尾 Z 归一为 UTC）。"""
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def lag_seconds(event_ts: str, land_ts: datetime) -> int:
    """落地时刻减事件时间，秒；event_ts 晚于落地（时钟漂移）可为负，不截断。"""
    return int((land_ts - parse_event_ts(event_ts)).total_seconds())


def percentile(sorted_values: list[int], pct: float) -> int | None:
    """nearest-rank 分位数；空样本返回 None。pct 取 (0,100]。"""
    if not sorted_values:
        return None
    rank = max(1, min(len(sorted_values), round(len(sorted_values) * pct / 100)))
    return sorted_values[rank - 1]


def summarize_lag(lags: list[int], threshold_seconds: int) -> dict[str, Any]:
    """滞后分布摘要：样本数、中位/P95、最大最小、超阈条数。"""
    ordered = sorted(lags)
    return {
        "sample_count": len(ordered),
        "lag_min_seconds": ordered[0] if ordered else None,
        "lag_median_seconds": percentile(ordered, 50),
        "lag_p95_seconds": percentile(ordered, 95),
        "lag_max_seconds": ordered[-1] if ordered else None,
        "threshold_seconds": int(threshold_seconds),
        "over_threshold_count": sum(1 for v in ordered if v > threshold_seconds),
    }


def resolve_config(
    threshold_raw: str | int | None,
    policy_raw: str | None,
) -> tuple[int, str]:
    """读取并校验可保存的 SLA 配置；非法值回落默认并显式报错由调用方决定。"""
    if threshold_raw is None or str(threshold_raw) == "":
        threshold = DEFAULT_THRESHOLD_SECONDS
    else:
        try:
            threshold = int(threshold_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid lag_sla threshold: {threshold_raw!r}") from exc
    if threshold <= 0:
        raise ValueError(f"lag_sla threshold must be positive, got {threshold}")

    policy = policy_raw or DEFAULT_POLICY
    if policy not in POLICIES:
        raise ValueError(f"invalid lag_sla policy: {policy!r}, expected one of {POLICIES}")
    return threshold, policy


def decide_action(*, over_threshold_count: int, policy: str) -> str:
    """互斥策略裁决：无违约放行；违约时冻结或记票，二选一。"""
    if over_threshold_count <= 0:
        return "advance_ok"
    if policy == POLICY_TICKET:
        return "advance_ok"
    return "freeze_cursor"
