from __future__ import annotations

from typing import Any

from app.config import resolve_fixture_path
from app.db import connect
from app.engines.apply_rules import classify_event
from app.engines.parse_change import parse_change_fixture
from app.modules.lag_sla_breach import (
    POLICY_FREEZE,
    SETTING_POLICY,
    SETTING_THRESHOLD,
    decide_action,
    lag_seconds,
    resolve_config,
    summarize_lag,
    utc_now,
)
from app.repositories import cursors as cursor_repo
from app.repositories import lag_sla as lag_repo
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import settings as settings_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo


def preview_stream(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        path = resolve_fixture_path(stream["fixture_path"])
        events = parse_change_fixture(path)
        return {"stream": stream, "count": len(events), "events": events[:50]}
    finally:
        conn.close()


def run_apply(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")

        cursor_before = int(stream["apply_cursor"] or 0)
        gap_policy = settings_repo.get(conn, "gap_policy", "stop_quarantine") or "stop_quarantine"
        threshold, policy = resolve_config(
            settings_repo.get(conn, SETTING_THRESHOLD),
            settings_repo.get(conn, SETTING_POLICY),
        )

        run_id = runs_repo.create_run(conn, stream_id, cursor_before)
        path = resolve_fixture_path(stream["fixture_path"])

        try:
            events = parse_change_fixture(path)
        except Exception as exc:  # noqa: BLE001
            runs_repo.finish_run(
                conn,
                run_id,
                status="failed",
                loaded=0,
                applied=0,
                quarantined=0,
                skipped=0,
                cursor_after=cursor_before,
                report={"error": str(exc)},
            )
            conn.commit()
            raise

        # ---------- 阶段 1：分析（不写任何业务表） ----------
        land_dt = utc_now()
        land_ts = land_dt.isoformat().replace("+00:00", "Z")
        plan_cursor = cursor_before
        plan: list[dict[str, Any]] = []
        stopped_on_gap = False

        for event in events:
            already = cursor_repo.is_applied(conn, stream_id, int(event["seq"]))
            decision = classify_event(
                event=event,
                cursor=plan_cursor,
                already_applied=already,
                gap_policy=gap_policy,
            )
            if decision == "skipped_duplicate":
                plan.append({"event": event, "decision": decision})
                continue
            if decision == "quarantine_gap":
                plan.append({"event": event, "decision": decision})
                stopped_on_gap = True
                break
            lag = lag_seconds(event["event_ts"], land_dt)
            plan.append(
                {
                    "event": event,
                    "decision": "apply",
                    "lag_seconds": lag,
                    "over_threshold": lag > threshold,
                }
            )
            plan_cursor = int(event["seq"])

        candidates = [p for p in plan if p["decision"] == "apply"]
        lags = [p["lag_seconds"] for p in candidates]
        summary = summarize_lag(lags, threshold)
        breach = summary["over_threshold_count"] > 0
        action = decide_action(over_threshold_count=summary["over_threshold_count"], policy=policy)
        frozen = action == POLICY_FREEZE

        # ---------- 阶段 2：按互斥策略执行（同一事务提交） ----------
        applied = 0
        quarantined = 0
        skipped = 0
        cursor = cursor_before
        dispositions: list[str] = []

        for item in plan:
            event = item["event"]
            decision = item["decision"]

            if decision == "skipped_duplicate":
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                skipped += 1
                dispositions.append(decision)
                continue

            if decision == "quarantine_gap":
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                quarantine_repo.insert(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    seq=int(event["seq"]),
                    entity_key=event["entity_key"],
                    reason="gap_before_seq",
                    payload=event.get("payload") or {},
                )
                quarantined += 1
                dispositions.append(decision)
                break

            # decision == apply
            if frozen:
                # 冻结策略：本批拟应用事件一律不写 Sink、不抬游标、不记 applied_seqs，
                # 仅留账本与违约票，保证游标不高于已提交水位。
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition="lag_frozen",
                )
                if event["op"] == "upsert":
                    sink_repo.upsert(
                        conn,
                        stream_id=stream_id,
                        entity_key=event["entity_key"],
                        payload=event.get("payload") or {},
                        seq=int(event["seq"]),
                        event_ts=event["event_ts"],
                    )
                else:
                    sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
                cursor_repo.mark_applied(conn, stream_id, int(event["seq"]), run_id)
                cursor = int(event["seq"])
                streams_repo.set_cursor(conn, stream_id, cursor)
                if not item["over_threshold"]:
                    lag_repo.insert_ticket(
                        conn,
                        run_id=run_id,
                        stream_id=stream_id,
                        seq=int(event["seq"]),
                        entity_key=event["entity_key"],
                        event_ts=event["event_ts"],
                        land_ts=land_ts,
                        lag_seconds=item["lag_seconds"],
                        threshold_seconds=threshold,
                        policy=policy,
                        action=action,
                    )
                dispositions.append("lag_frozen")
                continue

            if event["op"] == "upsert":
                sink_repo.upsert(
                    conn,
                    stream_id=stream_id,
                    entity_key=event["entity_key"],
                    payload=event.get("payload") or {},
                    seq=int(event["seq"]),
                    event_ts=event["event_ts"],
                )
            else:
                sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])

            cursor_repo.mark_applied(conn, stream_id, int(event["seq"]), run_id)
            cursor = int(event["seq"])
            streams_repo.set_cursor(conn, stream_id, cursor)
            ledger_repo.insert_ledger(
                conn, stream_id=stream_id, run_id=run_id, event=event, disposition="applied"
            )
            if item["over_threshold"]:
                # 记票放行：游标照常推进，但每条超阈事件都有可查违约票。
                lag_repo.insert_ticket(
                    conn,
                    run_id=run_id,
                    stream_id=stream_id,
                    seq=int(event["seq"]),
                    entity_key=event["entity_key"],
                    event_ts=event["event_ts"],
                    land_ts=land_ts,
                    lag_seconds=item["lag_seconds"],
                    threshold_seconds=threshold,
                    policy=policy,
                    action=action,
                )
            applied += 1
            dispositions.append("applied")

        cursor_advanced = cursor > cursor_before
        lag_repo.insert_run_stats(
            conn,
            run_id=run_id,
            stream_id=stream_id,
            land_ts=land_ts,
            threshold_seconds=threshold,
            policy=policy,
            summary=summary,
            breach=breach,
            action=action,
            cursor_advanced=cursor_advanced,
        )

        if frozen:
            status = "frozen"
        elif stopped_on_gap:
            status = "partial"
        else:
            status = "succeeded"

        report = {
            "stream_id": stream_id,
            "gap_policy": gap_policy,
            "dispositions": dispositions,
            "stopped_on_gap": stopped_on_gap,
            "sink_count_after": sink_repo.count_sink(conn),
            "quarantine_count_after": quarantine_repo.count_quarantine(conn),
            "lag_sla": {
                "land_ts": land_ts,
                "caliber": "lag_seconds = land_ts - event_ts; 样本仅含本批拟应用事件; 分位数 nearest-rank",
                "threshold_seconds": threshold,
                "policy": policy,
                "breach": breach,
                "action": action,
                "cursor_advanced": cursor_advanced,
                "summary": summary,
                "over_threshold_seqs": [
                    int(p["event"]["seq"]) for p in plan if p.get("over_threshold")
                ],
                "frozen_seqs": [
                    int(p["event"]["seq"]) for p in plan if frozen and p["decision"] == "apply"
                ],
            },
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=len(events),
            applied=applied,
            quarantined=quarantined,
            skipped=skipped,
            cursor_after=cursor,
            report=report,
        )
        conn.commit()
        return runs_repo.get_run(conn, run_id) or {}
    finally:
        conn.close()
