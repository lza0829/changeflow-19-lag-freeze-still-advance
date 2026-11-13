import json
from datetime import datetime, timedelta, timezone

from app.config import DATA_DIR
from app.db import connect
from app.modules.lag_sla_breach import (
    DEFAULT_POLICY,
    DEFAULT_THRESHOLD_SECONDS,
    SETTING_POLICY,
    SETTING_THRESHOLD,
)


def _write_jsonl(path, rows: list[dict]) -> None:
    path.write_text(
        "\n".join(json.dumps(e, ensure_ascii=False) for e in rows) + "\n",
        encoding="utf-8",
    )


def _freshen_base_fixtures() -> None:
    """首次初始化时把两个干净流复制到数据目录并刷新 event_ts。

    包内夹具时间固定会逐渐变旧，默认 600s 阈值下必然违约；这里保持 seq/op/entity/payload
    不变，仅把 event_ts 换成相对当前时刻的分钟级偏移，使干净流默认不违约。
    """
    fixture_dir = DATA_DIR / "fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    base = datetime.now(timezone.utc).replace(microsecond=0)

    def row(seq, op, entity_key, offset, payload):
        return {
            "seq": seq,
            "op": op,
            "entity_key": entity_key,
            "event_ts": (base + timedelta(seconds=offset))
            .isoformat()
            .replace("+00:00", "Z"),
            "payload": payload,
        }

    _write_jsonl(
        fixture_dir / "orders_cdc.jsonl",
        [
            row(1, "upsert", "ORD-1001", -240, {"status": "created", "amount": 120.5, "sku": "SKU-A"}),
            row(2, "upsert", "ORD-1001", -180, {"status": "paid", "amount": 120.5, "sku": "SKU-A"}),
            row(3, "upsert", "ORD-1002", -120, {"status": "created", "amount": 88.0, "sku": "SKU-B"}),
            row(4, "delete", "ORD-1002", -90, {}),
            row(5, "upsert", "ORD-1003", -60, {"status": "created", "amount": 45.0, "sku": "SKU-C"}),
        ],
    )
    _write_jsonl(
        fixture_dir / "inventory_cdc.jsonl",
        [
            row(1, "upsert", "INV-2001", -200, {"warehouse": "WH-E", "qty": 40, "sku": "SKU-A"}),
            row(2, "upsert", "INV-2001", -140, {"warehouse": "WH-E", "qty": 35, "sku": "SKU-A"}),
            row(3, "upsert", "INV-2002", -80, {"warehouse": "WH-W", "qty": 12, "sku": "SKU-B"}),
        ],
    )


def _ensure_lag_demo_fixture() -> str:
    """生成「可调出高滞后」的演示流：6 个连续事件，4 新鲜 / 2 高滞后。

    event_ts 相对生成时刻（≈落地时刻）计算，所以任何时间重跑都能稳定复现：
    默认阈值 600s 下，seq 2、5 为超阈事件，其余在阈内。
    """
    rel = "lag_demo_cdc.jsonl"
    fixture_dir = DATA_DIR / "fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    path = fixture_dir / rel
    base = datetime.now(timezone.utc).replace(microsecond=0)
    offsets = [-90, -3600, -240, -180, -7200, -60]  # 秒；-3600/-7200 超 600s 阈值
    events = []
    for seq, offset in enumerate(offsets, start=1):
        events.append(
            {
                "seq": seq,
                "op": "upsert",
                "entity_key": f"LAG-{1000 + seq}",
                "event_ts": (base + timedelta(seconds=offset))
                .isoformat()
                .replace("+00:00", "Z"),
                "payload": {"lag_demo": True, "offset_seconds": offset},
            }
        )
    path.write_text(
        "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n",
        encoding="utf-8",
    )
    return rel


def init_db():
    conn = connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS streams(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            fixture_path TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            apply_cursor INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS apply_runs(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            loaded_count INTEGER NOT NULL,
            applied_count INTEGER NOT NULL,
            quarantined_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            cursor_before INTEGER NOT NULL,
            cursor_after INTEGER NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS change_ledger(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER,
            seq INTEGER NOT NULL,
            op TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            disposition TEXT NOT NULL,
            UNIQUE(stream_id, seq, run_id)
        );
        CREATE TABLE IF NOT EXISTS sink_records(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL,
            last_event_ts TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(stream_id, entity_key)
        );
        CREATE TABLE IF NOT EXISTS quarantine(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS applied_seqs(
            stream_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            applied_at TEXT NOT NULL,
            PRIMARY KEY(stream_id, seq)
        );
        CREATE TABLE IF NOT EXISTS lag_run_stats(
            run_id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            land_ts TEXT NOT NULL,
            threshold_seconds INTEGER NOT NULL,
            policy TEXT NOT NULL,
            sample_count INTEGER NOT NULL,
            over_threshold_count INTEGER NOT NULL,
            breach INTEGER NOT NULL,
            action TEXT NOT NULL,
            lag_min_seconds INTEGER,
            lag_median_seconds INTEGER,
            lag_p95_seconds INTEGER,
            lag_max_seconds INTEGER,
            cursor_advanced INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS lag_breach_tickets(
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            stream_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            land_ts TEXT NOT NULL,
            lag_seconds INTEGER NOT NULL,
            threshold_seconds INTEGER NOT NULL,
            policy TEXT NOT NULL,
            action TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    if conn.execute("SELECT COUNT(*) c FROM streams").fetchone()["c"] == 0:
        _freshen_base_fixtures()
        lag_fixture = _ensure_lag_demo_fixture()
        conn.executemany(
            """
            INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    "Orders CDC",
                    "orders_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "clean：seq 1→5 连续；含一次 delete",
                ),
                (
                    "Inventory CDC",
                    "inventory_cdc.jsonl",
                    "inventory",
                    0,
                    1,
                    "clean：seq 1→3 连续库存变更",
                ),
                (
                    "Lag Demo CDC",
                    lag_fixture,
                    "lag_demo",
                    0,
                    1,
                    "滞后演示：6 连续事件，默认阈值 600s 下 2 条超阈（seq 2/5）",
                ),
            ],
        )
        conn.execute("INSERT INTO settings(key, value) VALUES ('project_label', 'Changeflow')")
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('gap_policy', 'stop_quarantine')"
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?)",
            (SETTING_THRESHOLD, str(DEFAULT_THRESHOLD_SECONDS)),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?)",
            (SETTING_POLICY, DEFAULT_POLICY),
        )
        conn.commit()
    else:
        # 既有库也要补齐滞后演示流与默认 SLA 配置（幂等）。
        if not (DATA_DIR / "fixtures" / "orders_cdc.jsonl").exists():
            _freshen_base_fixtures()
        exists = conn.execute(
            "SELECT 1 FROM streams WHERE fixture_path=?", ("lag_demo_cdc.jsonl",)
        ).fetchone()
        if not exists:
            lag_fixture = _ensure_lag_demo_fixture()
            conn.execute(
                """
                INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    "Lag Demo CDC",
                    lag_fixture,
                    "lag_demo",
                    0,
                    1,
                    "滞后演示：6 连续事件，默认阈值 600s 下 2 条超阈（seq 2/5）",
                ),
            )
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
            (SETTING_THRESHOLD, str(DEFAULT_THRESHOLD_SECONDS)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
            (SETTING_POLICY, DEFAULT_POLICY),
        )
        conn.commit()
    conn.close()
