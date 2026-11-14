from __future__ import annotations

import os
import tempfile

import pytest

# 每个测试进程使用独立临时库，避免污染开发库。
_TMP = tempfile.mkdtemp(prefix="changeflow_test_")
os.environ["DATA_DIR"] = _TMP

from app.db import connect  # noqa: E402
from app.modules.lag_sla_breach import (  # noqa: E402
    POLICY_FREEZE,
    POLICY_TICKET,
    decide_action,
    lag_seconds,
    percentile,
    resolve_config,
    summarize_lag,
    utc_now,
)
from app.repositories import cursors as cursor_repo  # noqa: E402
from app.repositories import lag_sla as lag_repo  # noqa: E402
from app.repositories import settings as settings_repo  # noqa: E402
from app.repositories import streams as streams_repo  # noqa: E402
from app.seed import init_db  # noqa: E402
from app.services import apply_service  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _seeded():
    init_db()
    yield


def _demo_stream_id(conn) -> int:
    row = conn.execute(
        "SELECT id FROM streams WHERE fixture_path='lag_demo_cdc.jsonl'"
    ).fetchone()
    return int(row["id"])


# ---------- 纯领域逻辑 ----------

def test_percentile_nearest_rank():
    assert percentile([], 95) is None
    vals = [10, 20, 30, 40, 50, 60]
    assert percentile(vals, 50) == 30
    assert percentile(vals, 95) == 60


def test_lag_seconds_land_minus_event():
    land = utc_now()
    from datetime import timedelta

    event = (land - timedelta(seconds=900)).isoformat().replace("+00:00", "Z")
    assert lag_seconds(event, land) == 900


def test_summary_marks_over_threshold():
    summary = summarize_lag([10, 700, 800, 30], 600)
    assert summary["sample_count"] == 4
    assert summary["over_threshold_count"] == 2
    assert summary["lag_median_seconds"] == 30
    assert summary["lag_p95_seconds"] == 800


def test_resolve_config_validates():
    assert resolve_config("120", POLICY_TICKET) == (120, POLICY_TICKET)
    assert resolve_config(None, None) == (600, POLICY_FREEZE)
    with pytest.raises(ValueError):
        resolve_config("0", POLICY_FREEZE)
    with pytest.raises(ValueError):
        resolve_config("600", "nonsense")


def test_decide_action_is_mutually_exclusive():
    assert decide_action(over_threshold_count=0, policy=POLICY_FREEZE) == "advance_ok"
    assert decide_action(over_threshold_count=2, policy=POLICY_FREEZE) == "freeze_cursor"
    assert decide_action(over_threshold_count=2, policy=POLICY_TICKET) == "ticket_and_advance"


# ---------- 端到端：冻结 / 记票 / 无违约 三态同源 ----------

def test_freeze_policy_blocks_cursor_and_writes_tickets():
    conn = connect()
    try:
        stream_id = _demo_stream_id(conn)
        settings_repo.put(conn, "lag_sla_threshold_seconds", "600")
        settings_repo.put(conn, "lag_sla_policy", POLICY_FREEZE)
        conn.commit()
        cursor_before = int(streams_repo.get_stream(conn, stream_id)["apply_cursor"])

        run = apply_service.run_apply(stream_id)

        assert run["status"] == "frozen"
        assert run["cursor_after"] == cursor_before  # 游标未抬高
        assert run["applied_count"] == 0
        lag = run["report"]["lag_sla"]
        assert lag["breach"] is True
        assert lag["action"] == "freeze_cursor"
        assert lag["cursor_advanced"] is False
        assert lag["summary"]["sample_count"] == 6
        assert lag["summary"]["over_threshold_count"] == 2
        assert sorted(lag["over_threshold_seqs"]) == [2, 5]
        assert sorted(lag["frozen_seqs"]) == [1, 2, 3, 4, 5, 6]

        stats = lag_repo.get_run_stats(conn, run["id"])
        assert stats["breach"] is True
        assert stats["action"] == "freeze_cursor"
        assert stats["cursor_advanced"] is False
        # 摘要与报告同源
        assert stats["sample_count"] == lag["summary"]["sample_count"]
        assert stats["over_threshold_count"] == lag["summary"]["over_threshold_count"]

        tickets = lag_repo.list_tickets(conn, run_id=run["id"])
        assert len(tickets) == 2
        assert sorted(t["seq"] for t in tickets) == [2, 5]
        assert all(t["action"] == "freeze_cursor" for t in tickets)

        # 冻结互证：Sink 零增量、applied_seqs 零记录、账本全为 lag_frozen
        sink_rows = conn.execute(
            "SELECT COUNT(*) c FROM sink_records WHERE stream_id=?", (stream_id,)
        ).fetchone()["c"]
        applied_rows = conn.execute(
            "SELECT COUNT(*) c FROM applied_seqs WHERE stream_id=?", (stream_id,)
        ).fetchone()["c"]
        frozen_ledger = conn.execute(
            "SELECT COUNT(*) c FROM change_ledger WHERE run_id=? AND disposition='lag_frozen'",
            (run["id"],),
        ).fetchone()["c"]
        assert sink_rows == 0
        assert applied_rows == 0
        assert frozen_ledger == 6

        # 游标页能看到冻结标记
        view = {r["stream_id"]: r for r in cursor_repo.list_cursor_view(conn)}
        assert view[stream_id]["lag_frozen"] is True
        assert view[stream_id]["apply_cursor"] == cursor_before

        # 冻结未抬水位：同配置重跑仍是 6 条拟应用、2 张票、继续冻结
        rerun = apply_service.run_apply(stream_id)
        assert rerun["status"] == "frozen"
        assert rerun["cursor_after"] == cursor_before
        assert rerun["report"]["lag_sla"]["summary"]["sample_count"] == 6
        assert len(lag_repo.list_tickets(conn, run_id=rerun["id"])) == 2
    finally:
        conn.close()


def test_ticket_policy_advances_cursor_and_keeps_tickets():
    conn = connect()
    try:
        stream_id = _demo_stream_id(conn)
        settings_repo.put(conn, "lag_sla_threshold_seconds", "600")
        settings_repo.put(conn, "lag_sla_policy", POLICY_TICKET)
        conn.commit()
        cursor_before = int(streams_repo.get_stream(conn, stream_id)["apply_cursor"])

        run = apply_service.run_apply(stream_id)

        assert run["status"] == "succeeded"
        assert run["applied_count"] == 6
        assert run["cursor_after"] == cursor_before + 6  # 游标推进
        lag = run["report"]["lag_sla"]
        assert lag["breach"] is True
        assert lag["action"] == "ticket_and_advance"
        assert lag["cursor_advanced"] is True

        stats = lag_repo.get_run_stats(conn, run["id"])
        assert stats["action"] == "ticket_and_advance"
        assert stats["cursor_advanced"] is True

        tickets = lag_repo.list_tickets(conn, run_id=run["id"])
        assert len(tickets) == 2
        assert sorted(t["seq"] for t in tickets) == [2, 5]
        assert all(t["action"] == "ticket_and_advance" for t in tickets)

        # 记票放行互证：Sink/applied_seqs/账本均按 6 条落地，票只对应超阈事件
        sink_rows = conn.execute(
            "SELECT COUNT(*) c FROM sink_records WHERE stream_id=?", (stream_id,)
        ).fetchone()["c"]
        applied_rows = conn.execute(
            "SELECT COUNT(*) c FROM applied_seqs WHERE stream_id=?", (stream_id,)
        ).fetchone()["c"]
        applied_ledger = conn.execute(
            "SELECT COUNT(*) c FROM change_ledger WHERE run_id=? AND disposition='applied'",
            (run["id"],),
        ).fetchone()["c"]
        assert sink_rows == 6
        assert applied_rows == 6
        assert applied_ledger == 6
        assert {t["seq"] for t in tickets} == set(lag["over_threshold_seqs"])

        view = {r["stream_id"]: r for r in cursor_repo.list_cursor_view(conn)}
        assert view[stream_id]["lag_frozen"] is False
        assert view[stream_id]["apply_cursor"] == cursor_before + 6
    finally:
        conn.close()


def test_fresh_stream_default_threshold_no_breach_advances_without_tickets():
    """干净流 event_ts 贴近落地时刻：默认阈值下不违约，正常推进且无违约票。"""
    conn = connect()
    try:
        orders_id = int(
            conn.execute("SELECT id FROM streams WHERE name='Orders CDC'").fetchone()["id"]
        )
        settings_repo.put(conn, "lag_sla_threshold_seconds", "600")
        settings_repo.put(conn, "lag_sla_policy", POLICY_FREEZE)
        conn.commit()
        threshold, policy = resolve_config(
            settings_repo.get(conn, "lag_sla_threshold_seconds"),
            settings_repo.get(conn, "lag_sla_policy"),
        )
        assert threshold == 600
        assert policy == POLICY_FREEZE
        cursor_before = int(streams_repo.get_stream(conn, orders_id)["apply_cursor"])

        run = apply_service.run_apply(orders_id)

        assert run["status"] == "succeeded"
        assert run["cursor_after"] == cursor_before + 5
        lag = run["report"]["lag_sla"]
        assert lag["breach"] is False
        assert lag["action"] == "advance_ok"
        assert lag["summary"]["sample_count"] == 5
        assert lag["summary"]["over_threshold_count"] == 0
        assert lag_repo.list_tickets(conn, run_id=run["id"]) == []
        stats = lag_repo.get_run_stats(conn, run["id"])
        assert stats["breach"] is False
        assert stats["cursor_advanced"] is True
    finally:
        conn.close()


def test_config_and_stats_survive_new_connection():
    """刷新后仍在：配置、滞后摘要、违约票都落 SQLite，新连接读出来一致。"""
    conn = connect()
    try:
        # 新连接读出的配置与最近一次写入一致（刷新后仍在）
        threshold, policy = resolve_config(
            settings_repo.get(conn, "lag_sla_threshold_seconds"),
            settings_repo.get(conn, "lag_sla_policy"),
        )
        assert threshold == 600
        assert policy in (POLICY_FREEZE, POLICY_TICKET)

        all_stats = lag_repo.list_run_stats(conn)
        assert len(all_stats) >= 3
        # 每次 run 恰好一条滞后摘要，且与 runs 表数量对齐
        run_count = int(
            conn.execute("SELECT COUNT(*) c FROM apply_runs").fetchone()["c"]
        )
        assert len(all_stats) == run_count

        all_tickets = lag_repo.list_tickets(conn)
        # 冻结 2 张 + 重跑冻结 2 张 + 记票 2 张 = 6
        assert len(all_tickets) == 6
        actions = {t["action"] for t in all_tickets}
        assert actions == {POLICY_FREEZE, POLICY_TICKET}
    finally:
        conn.close()
