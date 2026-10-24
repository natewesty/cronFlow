"""Warehouse I/O for the forecasting engine. All SQL lives here."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from .config import ACTIVE_MEMBERS_KPI, ALL_KPIS, ENGINE_VERSION, FORECAST_SCHEMA, KPI_SCHEMA

SCHEMA_SQL = Path(__file__).with_name("schema.sql")
CHUNK = 2000


# --- connection -------------------------------------------------------------

def database_url() -> str:
    """Mirror ingest.py: DATABASE_URL first (sslmode for Render), else DB_* parts."""
    url = os.getenv("DATABASE_URL")
    if url:
        if "render.com" in url and "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return url
    host, name = os.getenv("DB_HOST"), os.getenv("DB_NAME")
    user, pwd = os.getenv("DB_USER"), os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT") or "5432"
    if not all([host, name, user, pwd]):
        raise RuntimeError("No database configuration: set DATABASE_URL or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD")
    ssl = "?sslmode=require" if "render.com" in host else ""
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}{ssl}"


def get_engine(url: Optional[str] = None) -> Engine:
    return create_engine(url or database_url(), pool_pre_ping=True,
                         connect_args={"application_name": "dashdon_forecast", "connect_timeout": 30})


def ensure_schema(conn: Connection) -> None:
    for stmt in SCHEMA_SQL.read_text().split(";"):
        if stmt.strip():
            conn.execute(text(stmt))


# --- pure converters ------------------------------------------------------------

def frame_from_rows(rows: Iterable[Tuple], origin: pd.Timestamp) -> pd.DataFrame:
    """(date_key, kpi_id, value) rows -> wide daily frame through `origin`.
    Flows fill with 0, the member stock (23) forward-fills."""
    df = pd.DataFrame(list(rows), columns=["date_key", "kpi_id", "value"])
    if df.empty:
        return pd.DataFrame(columns=ALL_KPIS, dtype=float)
    df["date_key"] = pd.to_datetime(df["date_key"])
    df["kpi_id"] = df["kpi_id"].astype(int)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    wide = df.pivot_table(index="date_key", columns="kpi_id", values="value", aggfunc="sum")
    idx = pd.date_range(wide.index.min(), origin, freq="D")
    wide = wide.reindex(index=idx, columns=ALL_KPIS)
    if ACTIVE_MEMBERS_KPI in wide:
        wide[ACTIVE_MEMBERS_KPI] = wide[ACTIVE_MEMBERS_KPI].ffill()
    wide.index.name = None
    return wide.fillna(0.0).astype(float)


def _num(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def daily_records(point: pd.DataFrame, p10: pd.DataFrame, p90: pd.DataFrame,
                  comps: Dict[int, pd.DataFrame], run_id: int) -> List[dict]:
    out: List[dict] = []
    for h, d in enumerate(point.index, start=1):
        dkey = d.strftime("%Y-%m-%d")
        for k in point.columns:
            c = comps.get(k)
            otb = None
            compd: Dict[str, float] = {}
            if c is not None and d in c.index:
                for col in c.columns:
                    val = _num(c.loc[d, col])
                    if col == "otb":
                        otb = val
                    elif val is not None:
                        compd[col] = val
            out.append({
                "run_id": run_id, "kpi_id": int(k), "entity_id": 0, "date_key": dkey, "horizon": h,
                "p50": _num(point.loc[d, k]) or 0.0,
                "p10": _num(p10.loc[d, k]) if k in p10 else None,
                "p90": _num(p90.loc[d, k]) if k in p90 else None,
                "otb": otb, "components": json.dumps(compd),
            })
    return out


def monthly_records(monthly: pd.DataFrame, run_id: int) -> List[dict]:
    out = []
    for r in monthly.itertuples(index=False):
        out.append({
            "run_id": run_id, "kpi_id": int(r.kpi_id), "entity_id": 0,
            "month_start": pd.Timestamp(r.month_start).strftime("%Y-%m-%d"),
            "is_actual": bool(r.is_actual), "organic": _num(r.organic),
            "organic_neutral": _num(r.organic_neutral), "release": _num(r.release),
            "event": _num(r.event), "fitted_growth": _num(r.fitted_growth), "p50": _num(r.p50) or 0.0,
        })
    return out


def accuracy_records(accuracy: pd.DataFrame, run_id: int) -> List[dict]:
    out = []
    for r in accuracy.itertuples(index=False):
        out.append({
            "run_id": run_id, "kpi_id": int(r.kpi_id), "horizon_bucket": str(r.horizon_bucket),
            "wmape": _num(r.wmape), "bias": _num(r.bias), "ratio_p10": _num(r.ratio_p10),
            "ratio_p90": _num(r.ratio_p90), "weekly_wmape": _num(r.weekly_wmape),
            "monthly_wmape": _num(r.monthly_wmape), "n": int(r.n),
        })
    return out


# --- loaders ----------------------------------------------------------------------

def load_history(conn: Connection, origin: pd.Timestamp) -> pd.DataFrame:
    rows = conn.execute(text(f"""
        select date_key, kpi_id, value
        from {KPI_SCHEMA}.fact_kpi_daily
        where entity_id = 0 and date_key <= :origin
    """), {"origin": origin.strftime("%Y-%m-%d")}).fetchall()
    return frame_from_rows(rows, origin)


def load_otb(conn: Connection, origin: pd.Timestamp) -> pd.DataFrame:
    rows = conn.execute(text("""
        select date(r.reservation_datetime) as reservation_date,
               coalesce(e.attribution, 'Unattributed') as attribution,
               count(*) as reservations,
               coalesce(sum(r.party_size), 0) as guests,
               coalesce(sum(r.final_total), 0) as fees
        from public.fct_tock_reservation r
        left join public.dim_experience e on e.experience = r.experience_name
        where date(r.reservation_datetime) > :origin
        group by 1, 2
        order by 1, 2
    """), {"origin": origin.strftime("%Y-%m-%d")}).fetchall()
    df = pd.DataFrame(rows, columns=["reservation_date", "attribution", "reservations", "guests", "fees"])
    if not df.empty:
        df["reservation_date"] = pd.to_datetime(df["reservation_date"])
        for c in ["reservations", "guests", "fees"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def load_members_at(conn: Connection, origin: pd.Timestamp) -> Optional[float]:
    row = conn.execute(text("""
        select total_active_club_membership from public.agg_club_membership_active_daily
        where date_day <= :origin order by date_day desc limit 1
    """), {"origin": origin.strftime("%Y-%m-%d")}).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def load_assumptions(conn: Connection) -> dict:
    rows = conn.execute(text(f"select key, value from {FORECAST_SCHEMA}.assumptions")).fetchall()
    out = {}
    for key, value in rows:
        out[key] = json.loads(value) if isinstance(value, str) else value
    return out


# --- run lifecycle ----------------------------------------------------------------

def start_run(conn: Connection, origin: pd.Timestamp, horizon: int, params: dict) -> int:
    row = conn.execute(text(f"""
        insert into {FORECAST_SCHEMA}.forecast_runs (status, origin_date, horizon_days, engine_version, params)
        values ('running', :origin, :horizon, :version, cast(:params as jsonb))
        returning run_id
    """), {"origin": origin.strftime("%Y-%m-%d"), "horizon": int(horizon),
           "version": ENGINE_VERSION, "params": json.dumps(params)}).fetchone()
    return int(row[0])


def _insert_many(conn: Connection, sql: str, records: List[dict]) -> None:
    for i in range(0, len(records), CHUNK):
        conn.execute(text(sql), records[i:i + CHUNK])


def write_results(conn: Connection, run_id: int, daily: List[dict], monthly: List[dict],
                  accuracy: List[dict]) -> None:
    if daily:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.forecast_daily
              (run_id, kpi_id, entity_id, date_key, horizon, p50, p10, p90, otb, components)
            values (:run_id, :kpi_id, :entity_id, :date_key, :horizon, :p50, :p10, :p90, :otb, cast(:components as jsonb))
        """, daily)
    if monthly:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.forecast_monthly
              (run_id, kpi_id, entity_id, month_start, is_actual, organic, organic_neutral, release, event, fitted_growth, p50)
            values (:run_id, :kpi_id, :entity_id, :month_start, :is_actual, :organic, :organic_neutral, :release, :event, :fitted_growth, :p50)
        """, monthly)
    if accuracy:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.forecast_accuracy
              (run_id, kpi_id, horizon_bucket, wmape, bias, ratio_p10, ratio_p90, weekly_wmape, monthly_wmape, n)
            values (:run_id, :kpi_id, :horizon_bucket, :wmape, :bias, :ratio_p10, :ratio_p90, :weekly_wmape, :monthly_wmape, :n)
        """, accuracy)


def finish_run(conn: Connection, run_id: int, status: str, error: Optional[str] = None) -> None:
    if status == "success":
        conn.execute(text(f"update {FORECAST_SCHEMA}.forecast_runs set is_latest = false where is_latest"))
    conn.execute(text(f"""
        update {FORECAST_SCHEMA}.forecast_runs
        set status = :status, finished_at = now(), error = :error, is_latest = :latest
        where run_id = :run_id
    """), {"status": status, "error": error, "latest": status == "success", "run_id": run_id})


def write_bookings_snapshot(conn: Connection, snapshot_date: pd.Timestamp, otb: pd.DataFrame) -> None:
    if otb is None or otb.empty:
        return
    records = [{
        "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
        "reservation_date": pd.Timestamp(r.reservation_date).strftime("%Y-%m-%d"),
        "attribution": str(r.attribution), "reservations": int(r.reservations),
        "guests": int(r.guests), "fees": float(r.fees),
    } for r in otb.itertuples(index=False)]
    _insert_many(conn, f"""
        insert into {FORECAST_SCHEMA}.bookings_snapshot
          (snapshot_date, reservation_date, attribution, reservations, guests, fees)
        values (:snapshot_date, :reservation_date, :attribution, :reservations, :guests, :fees)
        on conflict (snapshot_date, reservation_date, attribution) do update
          set reservations = excluded.reservations, guests = excluded.guests, fees = excluded.fees
    """, records)


def prune_runs(conn: Connection, keep_days: int = 400) -> None:
    conn.execute(text(f"""
        delete from {FORECAST_SCHEMA}.forecast_runs
        where started_at < now() - make_interval(days => :days) and not is_latest
    """), {"days": int(keep_days)})
