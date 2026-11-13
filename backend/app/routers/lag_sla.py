from fastapi import APIRouter, HTTPException

from app.db import connect
from app.modules.lag_sla_breach import (
    POLICIES,
    SETTING_POLICY,
    SETTING_THRESHOLD,
    resolve_config,
)
from app.repositories import lag_sla as lag_repo
from app.repositories import runs as runs_repo
from app.repositories import settings as settings_repo
from app.schemas.entities import LagSlaConfigPut

router = APIRouter(tags=["lag_sla"])


@router.get("/lag-sla/config")
def get_config():
    conn = connect()
    try:
        threshold, policy = resolve_config(
            settings_repo.get(conn, SETTING_THRESHOLD),
            settings_repo.get(conn, SETTING_POLICY),
        )
        return {
            SETTING_THRESHOLD: threshold,
            SETTING_POLICY: policy,
            "policies": list(POLICIES),
        }
    finally:
        conn.close()


@router.put("/lag-sla/config")
def put_config(body: LagSlaConfigPut):
    try:
        threshold, policy = resolve_config(body.threshold_seconds, body.policy)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    conn = connect()
    try:
        settings_repo.put(conn, SETTING_THRESHOLD, str(threshold))
        settings_repo.put(conn, SETTING_POLICY, policy)
        conn.commit()
        return {
            SETTING_THRESHOLD: threshold,
            SETTING_POLICY: policy,
            "policies": list(POLICIES),
        }
    finally:
        conn.close()


@router.get("/lag-sla/runs")
def list_lag_runs(limit: int = 100):
    conn = connect()
    try:
        return lag_repo.list_run_stats(conn, limit=limit)
    finally:
        conn.close()


@router.get("/lag-sla/runs/{run_id}")
def get_lag_run(run_id: int):
    conn = connect()
    try:
        stats = lag_repo.get_run_stats(conn, run_id)
        if not stats:
            raise HTTPException(404, "lag stats not found for run")
        run = runs_repo.get_run(conn, run_id)
        tickets = lag_repo.list_tickets(conn, run_id=run_id, limit=1000)
        return {"stats": stats, "run": run, "tickets": tickets}
    finally:
        conn.close()


@router.get("/lag-sla/tickets")
def list_tickets(stream_id: int | None = None, run_id: int | None = None):
    conn = connect()
    try:
        return lag_repo.list_tickets(conn, stream_id=stream_id, run_id=run_id)
    finally:
        conn.close()
