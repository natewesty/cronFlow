"""One forecasting run: load → model → backtest → write (spec §7)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from . import io as fio
from .assumptions import parse_events, parse_params, parse_schedule
from . import backtest as fbt
from .config import ALL_KPIS, ENGINE_VERSION
from .engine import forecast_daily
from .models.club import default_schedule, detect_releases
from .monthly import monthly_forecast
from .types import Context

log = logging.getLogger("forecast")
PACIFIC = ZoneInfo("America/Los_Angeles")


@dataclass
class RunSummary:
    run_id: Optional[int]
    origin: pd.Timestamp
    horizon: int
    kpis: int
    daily_rows: int
    monthly_rows: int
    accuracy_rows: int
    next28_total_revenue: float
    period_rows: int = 0
    bottle_rows: int = 0


def plan_year(origin: pd.Timestamp) -> int:
    """The planning year the bottle and club plans are read for."""
    return int(origin.year) + 1


def today_pacific() -> pd.Timestamp:
    now = datetime.now(PACIFIC)
    return pd.Timestamp(now.year, now.month, now.day)


def run(origin: Optional[pd.Timestamp] = None, horizon: Optional[int] = None, dry_run: bool = False,
        skip_backtest: bool = False, url: Optional[str] = None) -> RunSummary:
    engine = fio.get_engine(url)
    with engine.begin() as conn:
        fio.ensure_schema(conn)

    origin = pd.Timestamp(origin).normalize() if origin is not None else today_pacific() - pd.Timedelta(days=1)
    live = today_pacific() - pd.Timedelta(days=1)
    if origin > live:
        raise ValueError(f"origin {origin.date()} is after the last complete day {live.date()}")
    with engine.connect() as conn:
        hist = fio.load_history(conn, origin)
        otb = fio.load_otb(conn, origin)
        members = fio.load_members_at(conn, origin)
        assumptions = fio.load_assumptions(conn)
    if hist.empty:
        raise RuntimeError("fact_kpi_daily returned no rows")

    params = parse_params(assumptions.get("engine_params"))
    py = plan_year(origin)
    with engine.connect() as conn:
        club_plan = fio.load_club_plan(conn, py)
        tier_shares = fio.load_tier_shares(conn, origin, params.tier_min_members)
        label_plan = fio.load_label_plan(conn, py)
        vintage_plan = fio.load_vintage_plan(conn)
    horizon = int(horizon or params.horizon)
    releases = detect_releases(hist[4], params)
    schedule = parse_schedule(assumptions.get("shipment_schedule")) or default_schedule(releases, origin)
    events = parse_events(assumptions.get("events"))
    a0 = float(members) if members is not None else float(hist[23].iloc[-1])
    ctx = Context(params=params, releases=releases, schedule=schedule, events=events, otb=otb, members_a0=a0,
                  club_plan=club_plan, tier_shares=tier_shares, label_plan=label_plan,
                  vintage_plan=vintage_plan)
    dates = pd.date_range(origin + pd.Timedelta(days=1), periods=horizon, freq="D")

    log.info("origin=%s horizon=%s history=%s..%s releases=%s scheduled=%s events=%s members=%s",
             origin.date(), horizon, hist.index[0].date(), hist.index[-1].date(),
             len(releases), len(schedule), len(events), a0)

    engine_params = params.to_dict()
    engine_params["horizon"] = horizon
    effective = {"engine": engine_params, "engine_version": ENGINE_VERSION,
                 "schedule": [{"start": s.start.strftime("%Y-%m-%d"), "days": s.days, "label": s.label} for s in schedule],
                 "events": events, "members_a0": a0, "releases_detected": len(releases)}
    run_id: Optional[int] = None
    if not dry_run:
        with engine.begin() as conn:
            run_id = fio.start_run(conn, origin, horizon, effective)

    try:
        point, comps = forecast_daily(hist, origin, dates, ctx)
        accuracy = pd.DataFrame(columns=fbt.ACCURACY_COLUMNS) if skip_backtest else fbt.run_backtest(hist, releases, params, origin)
        p10, p90 = fbt.apply_bands(point, accuracy, params)
        monthly = monthly_forecast(hist, origin, ctx, daily_point=point)
        # WS-B: periods
        # WS-B: bottles + clubs

        next28 = float(point[14].iloc[:28].sum())
        for k in ALL_KPIS:
            row = accuracy[(accuracy.kpi_id == k) & (accuracy.horizon_bucket == "8-28")]
            wm = f"{float(row.wmape.iloc[0]):.3f}" if len(row) and pd.notna(row.wmape.iloc[0]) else "n/a"
            log.info("kpi=%s next28=%.2f wmape_8_28=%s", k, float(point[k].iloc[:28].sum()), wm)

        daily_recs = fio.daily_records(point, p10, p90, comps, run_id=0)
        monthly_recs = fio.monthly_records(monthly, run_id=0)
        acc_recs = fio.accuracy_records(accuracy, run_id=0)
        summary = RunSummary(run_id=None, origin=origin, horizon=horizon, kpis=len(point.columns),
                             daily_rows=len(daily_recs), monthly_rows=len(monthly_recs),
                             accuracy_rows=len(acc_recs), next28_total_revenue=next28)
        if dry_run:
            log.info("dry run: %s", summary)
            return summary

        for rec in daily_recs + monthly_recs + acc_recs:
            rec["run_id"] = run_id
        with engine.begin() as conn:
            fio.write_results(conn, run_id, daily_recs, monthly_recs, acc_recs)
            if origin == live:
                fio.write_bookings_snapshot(conn, origin + pd.Timedelta(days=1), otb)
            else:
                log.info("bookings snapshot skipped: backfill origin %s; the OTB table reflects today, not the origin",
                         origin.date())
            fio.finish_run(conn, run_id, "success")
            fio.prune_runs(conn)
    except Exception as exc:  # noqa: BLE001 - we want to record any failure
        if run_id is not None:
            with engine.begin() as conn:
                fio.finish_run(conn, run_id, "failed", str(exc))
        raise
    summary.run_id = run_id
    log.info("run %s complete: %s", run_id, summary)
    return summary
