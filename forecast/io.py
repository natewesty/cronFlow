"""Warehouse I/O for the forecasting engine. All SQL lives here."""
from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from .bottles import label_from_title, varietal_from_title, vintage_from_title
from .clubs import take_rate_default, tier_table
from .config import ACTIVE_MEMBERS_KPI, ALL_KPIS, ENGINE_VERSION, FORECAST_SCHEMA, KPI_SCHEMA

SCHEMA_SQL = Path(__file__).with_name("schema.sql")
CHUNK = 1000          # rows per multi-row INSERT (11 cols x 1000 stays under Postgres' 65535 binds)


# --- connection -------------------------------------------------------------

def database_url() -> str:
    """Mirror ingest.py: DATABASE_URL first (sslmode for Render), else DB_* parts."""
    url = os.getenv("DATABASE_URL")
    if url:
        if "render.com" in url and "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return _with_psycopg2(url)
    host, name = os.getenv("DB_HOST"), os.getenv("DB_NAME")
    user, pwd = os.getenv("DB_USER"), os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT") or "5432"
    if not all([host, name, user, pwd]):
        raise RuntimeError("No database configuration: set DATABASE_URL or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD")
    ssl = "?sslmode=require" if "render.com" in host else ""
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}{ssl}"


def _with_psycopg2(url: str) -> str:
    """SQLAlchemy 2.1 made psycopg (v3) the default driver for bare postgresql:// URLs;
    this project installs psycopg2, so name it explicitly."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url


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
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                pass  # malformed entry: keep the raw value, downstream parsers ignore it
        out[key] = value
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


_PARAM = re.compile(r"(?<![:\w]):(\w+)")


def expand_values(sql: str, n: int) -> str:
    """Repeat the statement's `values (...)` tuple `n` times, suffixing each bind
    parameter with the row index (`:p` -> `:p_0, :p_1, ...`), so a chunk is sent as
    one multi-row INSERT instead of one round trip per row."""
    m = re.search(r"\bvalues\s*\(", sql, flags=re.IGNORECASE)
    if not m:
        raise ValueError("insert statement has no VALUES tuple")
    start, depth, i = m.end() - 1, 0, m.end() - 1
    while i < len(sql):
        depth += {"(": 1, ")": -1}.get(sql[i], 0)
        if depth == 0:
            break
        i += 1
    tpl, tail = sql[start:i + 1], sql[i + 1:]
    rows = ", ".join(_PARAM.sub(lambda p, j=j: f":{p.group(1)}_{j}", tpl) for j in range(n))
    return sql[:start] + rows + tail


def _insert_many(conn: Connection, sql: str, records: List[dict]) -> None:
    for i in range(0, len(records), CHUNK):
        chunk = records[i:i + CHUNK]
        params = {f"{k}_{j}": v for j, rec in enumerate(chunk) for k, v in rec.items()}
        conn.execute(text(expand_values(sql, len(chunk))), params)


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
        set status = :status, finished_at = clock_timestamp(), error = :error, is_latest = :latest
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


# --- Forecast v2: loader / writer contracts ----------------------------------
# Signatures are final. Bodies are stubs until WS-B fills them in.

EMPTY: Dict[str, List[str]] = {
    "club_plan": ["season", "release_date", "tier_id", "label", "vintage", "qty", "price",
                  "take_rate", "members_override"],
    "tier_shares": ["tier_id", "tier_name", "active", "share"],
    "label_plan": ["plan_year", "label", "status", "incoming_vintage", "release_month",
                   "override_outgoing", "override_incoming", "price", "varietal", "comparable_label"],
    "vintage_plan": ["label", "vintage", "cases_made", "assumed"],
    "bottle_history": ["label", "vintage", "varietal", "month_start", "channel", "bottles", "revenue", "price"],
    "club_orders": ["order_id", "customer_id", "created_at", "label", "vintage", "bottles", "revenue"],
    "memberships": ["membership_id", "customer_id", "club_id", "club_name", "status", "signup_at",
                    "cancel_at", "delivery_method"],
}


def _empty(name: str) -> pd.DataFrame:
    return pd.DataFrame(columns=list(EMPTY[name]))


def _empty_club_plan() -> pd.DataFrame:
    return _empty("club_plan")


def _empty_tier_shares() -> pd.DataFrame:
    return _empty("tier_shares")


def _empty_label_plan() -> pd.DataFrame:
    return _empty("label_plan")


def _empty_vintage_plan() -> pd.DataFrame:
    return _empty("vintage_plan")


def _empty_bottle_history() -> pd.DataFrame:
    return _empty("bottle_history")


def _empty_club_orders() -> pd.DataFrame:
    return _empty("club_orders")


def _empty_memberships() -> pd.DataFrame:
    return _empty("memberships")


NIL_UUID = "00000000-0000-0000-0000-000000000000"   # stored tier_id for the synthetic 'other' tier (column is uuid not null)
OTHER_TIER = "other"


def _date(v) -> Optional[str]:
    if v is None or pd.isna(v):
        return None
    return pd.Timestamp(v).strftime("%Y-%m-%d")


def _int(v) -> Optional[int]:
    f = _num(v)
    return None if f is None else int(round(f))


def _s(v) -> Optional[str]:
    return None if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT else str(v)


# --- v2 converters ---------------------------------------------------------------

def period_records(periods: pd.DataFrame, run_id: int) -> List[dict]:
    return [{
        "run_id": run_id, "kpi_id": int(r.kpi_id), "period": str(r.period), "ahead": int(r.ahead),
        "period_start": _date(r.period_start), "origin_date": _date(r.origin_date),
        "actual": _num(r.actual), "forecast": _num(r.forecast), "p10": _num(r.p10), "p90": _num(r.p90),
    } for r in periods.itertuples(index=False)]


def accuracy_period_records(acc: pd.DataFrame, run_id: int) -> List[dict]:
    return [{
        "run_id": run_id, "kpi_id": int(r.kpi_id), "period": str(r.period), "ahead": int(r.ahead),
        "wmape": _num(r.wmape), "bias": _num(r.bias), "hit_rate": _num(r.hit_rate), "n": _int(r.n),
    } for r in acc.itertuples(index=False)]


def bottle_records(fc: pd.DataFrame, run_id: int) -> List[dict]:
    return [{
        "run_id": run_id, "label": str(r.label), "vintage": int(r.vintage), "month_start": _date(r.month_start),
        "channel": str(r.channel), "p50": _num(r.p50) or 0.0, "p10": _num(r.p10), "p90": _num(r.p90),
    } for r in fc.itertuples(index=False)]


def vintage_summary_records(summ: pd.DataFrame, run_id: int) -> List[dict]:
    return [{
        "run_id": run_id, "label": str(r.label), "vintage": int(r.vintage), "varietal": _s(r.varietal),
        "release_month": _date(r.release_month), "sold_to_date": _int(r.sold_to_date),
        "cases_made": _int(r.cases_made), "lifetime_p50": _num(r.lifetime_p50),
        "lifetime_p10": _num(r.lifetime_p10), "lifetime_p90": _num(r.lifetime_p90),
    } for r in summ.itertuples(index=False)]


def profile_records(prof: pd.DataFrame, run_id: int) -> List[dict]:
    return [{
        "run_id": run_id, "scope": str(r.scope), "key": str(r.key), "channel": str(r.channel),
        "curve": json.dumps([_num(x) or 0.0 for x in r.curve]),
        "lifetime": _num(r.lifetime), "trend": _num(r.trend), "log_dispersion": _num(r.log_dispersion),
        "n_vintages": _int(r.n_vintages),
    } for r in prof.itertuples(index=False)]


def bottle_backtest_records(bt: pd.DataFrame, run_id: int) -> List[dict]:
    return [{
        "run_id": run_id, "label": str(r.label), "vintage": int(r.vintage),
        "first_year_actual": _num(r.first_year_actual), "first_year_forecast": _num(r.first_year_forecast),
        "p10": _num(r.p10), "p90": _num(r.p90),
    } for r in bt.itertuples(index=False)]


def club_history_records(hist: pd.DataFrame) -> List[dict]:
    """Tier id 'other' (clubs.OTHER) is stored as the nil uuid, because the column is `uuid not null`;
    the app treats NIL_UUID as the 'other' tier."""
    out = []
    for r in hist.itertuples(index=False):
        tid = str(r.tier_id)
        out.append({
            "release_start": _date(r.release_start), "season": str(r.season),
            "tier_id": NIL_UUID if tid == OTHER_TIER else tid, "tier_name": _s(r.tier_name),
            "label": str(r.label), "vintage": None if pd.isna(r.vintage) else int(r.vintage),
            "qty_per_box": _num(r.qty_per_box), "price": _num(r.price), "orders": _int(r.orders),
            "members": _int(r.members), "take_rate": _num(r.take_rate), "bottles": _int(r.bottles),
            "revenue": _num(r.revenue),
        })
    return out


# --- v2 loaders ------------------------------------------------------------------

def _frame(rows, cols: List[str]) -> pd.DataFrame:
    return pd.DataFrame([tuple(r) for r in rows], columns=cols)


def _parse_titles(df: pd.DataFrame) -> pd.DataFrame:
    """Add label / vintage / varietal from the raw title, preferring the warehouse vintage/varietal."""
    titles = df["title"].fillna("").astype(str)
    df = df.copy()
    df["label"] = titles.map(label_from_title)
    parsed = titles.map(vintage_from_title)
    df["vintage"] = pd.to_numeric(df["vintage"], errors="coerce").where(lambda s: s.notna(), pd.to_numeric(parsed, errors="coerce"))
    if "varietal" in df:
        fb = titles.map(varietal_from_title)
        df["varietal"] = df["varietal"].where(df["varietal"].notna(), fb)
    return df[df["vintage"].notna() & (df["label"] != "")].assign(vintage=lambda d: d["vintage"].astype(int))


def load_club_plan(conn: Connection, plan_year: int) -> pd.DataFrame:
    rows = conn.execute(text(f"""
        select p.season, p.release_date, cast(p.tier_id as text) as tier_id, p.label, p.vintage, p.qty,
               p.price, t.take_rate, t.members_override
        from {FORECAST_SCHEMA}.club_release_plan p
        left join {FORECAST_SCHEMA}.club_release_tier_plan t
          on t.plan_year = p.plan_year and t.season = p.season and t.tier_id = p.tier_id
        where p.plan_year = :y
        order by p.release_date, p.tier_id, p.label, p.vintage
    """), {"y": int(plan_year)}).fetchall()
    plan = _frame(rows, EMPTY["club_plan"])
    if plan.empty:
        return plan
    hrows = conn.execute(text(f"""
        select release_start, season, cast(tier_id as text) as tier_id, label, vintage, price, take_rate
        from {FORECAST_SCHEMA}.club_release_history
    """)).fetchall()
    hist = _frame(hrows, ["release_start", "season", "tier_id", "label", "vintage", "price", "take_rate"])
    for c in ["qty", "price", "take_rate", "members_override"]:
        plan[c] = pd.to_numeric(plan[c], errors="coerce")
    plan["release_date"] = pd.to_datetime(plan["release_date"])
    latest: Dict[str, float] = {}
    if not hist.empty:
        h = hist.dropna(subset=["price"]).sort_values("release_start")
        latest = h.groupby("label")["price"].last().astype(float).to_dict()
    plan["price"] = [p if pd.notna(p) else latest.get(lbl, 0.0) for p, lbl in zip(plan["price"], plan["label"])]
    plan["take_rate"] = [t if pd.notna(t) else take_rate_default(hist, tid, season)
                         for t, tid, season in zip(plan["take_rate"], plan["tier_id"], plan["season"])]
    return plan[EMPTY["club_plan"]]


def load_tier_shares(conn: Connection, origin: pd.Timestamp, min_members: int) -> pd.DataFrame:
    return tier_table(load_memberships(conn), origin, min_members)


def load_label_plan(conn: Connection, plan_year: int) -> pd.DataFrame:
    rows = conn.execute(text(f"""
        select plan_year, label, status, incoming_vintage, release_month, override_outgoing,
               override_incoming, price, varietal, comparable_label
        from {FORECAST_SCHEMA}.label_plan where plan_year = :y order by label
    """), {"y": int(plan_year)}).fetchall()
    df = _frame(rows, EMPTY["label_plan"])
    if not df.empty:
        df["release_month"] = pd.to_datetime(df["release_month"])
        for c in ["override_outgoing", "override_incoming", "price"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_vintage_plan(conn: Connection) -> pd.DataFrame:
    rows = conn.execute(text(f"""
        select label, vintage, cases_made, assumed from {FORECAST_SCHEMA}.bottle_vintage_plan
        order by label, vintage
    """)).fetchall()
    return _frame(rows, EMPTY["vintage_plan"])


def load_bottle_history(conn: Connection, origin: pd.Timestamp) -> pd.DataFrame:
    rows = conn.execute(text("""
        select date_trunc('month', i.paid_at)::date as month_start, p.title, p.vintage, p.varietal,
               i.channel, sum(i.quantity) as bottles, sum(i.product_subtotal) as revenue
        from public.fct_order_item i
        join public.stg_product p on p.product_id = i.product_id
        where i.item_type = 'Wine' and i.paid_at <= :origin
        group by 1, 2, 3, 4, 5
    """), {"origin": origin.strftime("%Y-%m-%d")}).fetchall()
    cols = EMPTY["bottle_history"]
    if not rows:
        return _empty("bottle_history")
    df = _frame(rows, ["month_start", "title", "vintage", "varietal", "channel", "bottles", "revenue"])
    df["bottles"] = pd.to_numeric(df["bottles"], errors="coerce").fillna(0.0)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0.0)
    df = _parse_titles(df)
    if df.empty:
        return _empty("bottle_history")
    df["month_start"] = pd.to_datetime(df["month_start"])
    df["varietal"] = df["varietal"].fillna("")          # keep NaN out of the group keys
    out = (df.groupby(["label", "vintage", "varietal", "month_start", "channel"], as_index=False)
             .agg(bottles=("bottles", "sum"), revenue=("revenue", "sum")))
    out["varietal"] = out["varietal"].replace("", None)
    out["price"] = [r / b if b > 0 else float("nan") for r, b in zip(out["revenue"], out["bottles"])]
    return out[cols].sort_values(["label", "vintage", "month_start", "channel"]).reset_index(drop=True)


def load_club_orders(conn: Connection, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows = conn.execute(text("""
        select o.order_id, o.customer_id, o.created_at, p.title, p.vintage,
               sum(i.quantity) as bottles, sum(i.product_subtotal) as revenue
        from public.fct_order o
        join public.fct_order_item i on i.order_id = o.order_id and i.item_type = 'Wine'
        join public.stg_product p on p.product_id = i.product_id
        where o.channel = 'Club' and o.created_at >= :start and o.created_at < :end
        group by o.order_id, o.customer_id, o.created_at, p.title, p.vintage
    """), {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")}).fetchall()
    if not rows:
        return _empty("club_orders")
    df = _frame(rows, ["order_id", "customer_id", "created_at", "title", "vintage", "bottles", "revenue"])
    df = _parse_titles(df)
    df["created_at"] = pd.to_datetime(df["created_at"])
    for c in ["bottles", "revenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    # two products can share label + vintage: collapse to one row per order x label x vintage
    out = (df.groupby(["order_id", "customer_id", "created_at", "label", "vintage"], as_index=False)
             .agg(bottles=("bottles", "sum"), revenue=("revenue", "sum")))
    return out[EMPTY["club_orders"]]


def load_memberships(conn: Connection) -> pd.DataFrame:
    rows = conn.execute(text("""
        select membership_id, customer_id, club_id, _membership_json->'club'->'seo'->>'title' as club_name,
               status, signup_at, cancel_at, delivery_method
        from public.dim_club_membership
    """)).fetchall()
    df = _frame(rows, EMPTY["memberships"])
    if not df.empty:
        df["club_id"] = df["club_id"].astype(str)
        for c in ["signup_at", "cancel_at"]:
            df[c] = pd.to_datetime(df[c])
    return df


def load_members_daily(conn: Connection, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    rows = conn.execute(text("""
        select date_day, total_active_club_membership from public.agg_club_membership_active_daily
        where date_day >= :start and date_day <= :end order by date_day
    """), {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")}).fetchall()
    if not rows:
        return pd.Series(dtype=float, index=pd.DatetimeIndex([]))
    idx = pd.DatetimeIndex(pd.to_datetime([r[0] for r in rows]))
    return pd.Series([_num(r[1]) for r in rows], index=idx, dtype=float)


# --- v2 writers ------------------------------------------------------------------

def write_periods(conn: Connection, run_id: int, period_recs: List[dict], acc_recs: List[dict]) -> None:
    if period_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.forecast_backtest_periods
              (run_id, kpi_id, period, ahead, period_start, origin_date, actual, forecast, p10, p90)
            values (:run_id, :kpi_id, :period, :ahead, :period_start, :origin_date, :actual, :forecast, :p10, :p90)
        """, period_recs)
    if acc_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.forecast_accuracy_periods
              (run_id, kpi_id, period, ahead, wmape, bias, hit_rate, n)
            values (:run_id, :kpi_id, :period, :ahead, :wmape, :bias, :hit_rate, :n)
        """, acc_recs)


def write_bottles(conn: Connection, run_id: int, forecast_recs: List[dict], summary_recs: List[dict],
                  profile_recs: List[dict], backtest_recs: List[dict]) -> None:
    if forecast_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.bottle_forecast
              (run_id, label, vintage, month_start, channel, p50, p10, p90)
            values (:run_id, :label, :vintage, :month_start, :channel, :p50, :p10, :p90)
        """, forecast_recs)
    if summary_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.bottle_vintage_summary
              (run_id, label, vintage, varietal, release_month, sold_to_date, cases_made,
               lifetime_p50, lifetime_p10, lifetime_p90)
            values (:run_id, :label, :vintage, :varietal, :release_month, :sold_to_date, :cases_made,
                    :lifetime_p50, :lifetime_p10, :lifetime_p90)
        """, summary_recs)
    if profile_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.bottle_profiles
              (run_id, scope, key, channel, curve, lifetime, trend, log_dispersion, n_vintages)
            values (:run_id, :scope, :key, :channel, cast(:curve as jsonb), :lifetime, :trend,
                    :log_dispersion, :n_vintages)
        """, profile_recs)
    if backtest_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.bottle_backtest
              (run_id, label, vintage, first_year_actual, first_year_forecast, p10, p90)
            values (:run_id, :label, :vintage, :first_year_actual, :first_year_forecast, :p10, :p90)
        """, backtest_recs)


def write_club_history(conn: Connection, history_recs: List[dict]) -> None:
    """Delete + insert (not run-scoped)."""
    conn.execute(text(f"delete from {FORECAST_SCHEMA}.club_release_history"))
    if history_recs:
        _insert_many(conn, f"""
            insert into {FORECAST_SCHEMA}.club_release_history
              (release_start, season, tier_id, tier_name, label, vintage, qty_per_box, price, orders,
               members, take_rate, bottles, revenue)
            values (:release_start, :season, cast(:tier_id as uuid), :tier_name, :label, :vintage, :qty_per_box,
                    :price, :orders, :members, :take_rate, :bottles, :revenue)
        """, history_recs)
