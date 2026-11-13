from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

_STAT_COLUMNS = (
    "run_id, stream_id, land_ts, threshold_seconds, policy, sample_count, "
    "over_threshold_count, breach, action, lag_min_seconds, lag_median_seconds, "
    "lag_p95_seconds, lag_max_seconds, cursor_advanced"
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def insert_run_stats(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    stream_id: int,
    land_ts: str,
    threshold_seconds: int,
    policy: str,
    summary: dict[str, Any],
    breach: bool,
    action: str,
    cursor_advanced: bool,
) -> None:
    conn.execute(
        f"""
        INSERT INTO lag_run_stats({_STAT_COLUMNS})
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            stream_id,
            land_ts,
            threshold_seconds,
            policy,
            summary["sample_count"],
            summary["over_threshold_count"],
            1 if breach else 0,
            action,
            summary["lag_min_seconds"],
            summary["lag_median_seconds"],
            summary["lag_p95_seconds"],
            summary["lag_max_seconds"],
            1 if cursor_advanced else 0,
        ),
    )


def insert_ticket(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    stream_id: int,
    seq: int,
    entity_key: str,
    event_ts: str,
    land_ts: str,
    lag_seconds: int,
    threshold_seconds: int,
    policy: str,
    action: str,
) -> None:
    conn.execute(
        """
        INSERT INTO lag_breach_tickets(
            run_id, stream_id, seq, entity_key, event_ts, land_ts, lag_seconds,
            threshold_seconds, policy, action, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            stream_id,
            seq,
            entity_key,
            event_ts,
            land_ts,
            lag_seconds,
            threshold_seconds,
            policy,
            action,
            _now(),
        ),
    )


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["breach"] = bool(item["breach"])
    item["cursor_advanced"] = bool(item["cursor_advanced"])
    return item


def list_run_stats(conn: sqlite3.Connection, limit: int = 100) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT {_STAT_COLUMNS} FROM lag_run_stats ORDER BY run_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_run_stats(conn: sqlite3.Connection, run_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        f"SELECT {_STAT_COLUMNS} FROM lag_run_stats WHERE run_id=?",
        (run_id,),
    ).fetchone()
    return _row_to_dict(row) if row else None


def list_tickets(
    conn: sqlite3.Connection,
    *,
    run_id: int | None = None,
    stream_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = (
        "SELECT id, run_id, stream_id, seq, entity_key, event_ts, land_ts, lag_seconds, "
        "threshold_seconds, policy, action, created_at FROM lag_breach_tickets WHERE 1=1"
    )
    params: list[Any] = []
    if run_id is not None:
        sql += " AND run_id=?"
        params.append(run_id)
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]
