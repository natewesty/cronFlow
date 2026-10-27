"""One forecasting run: load → model → backtest → write (spec §7)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from . import io as fio
from .assumptions import merge_schedule, parse_events, parse_params, parse_schedule
from . import backtest as fbt
from .bottles import backtest_vintages, fit_profiles, forecast_vintages, price_bands, vintage_table
from .clubs import release_history, tier_table
from .config import ALL_KPIS, ENGINE_VERSION, SELECTABLE_KPIS
from .engine import forecast_daily
from .models.club import default_schedule, detect_releases, mark_planned
from .monthly import monthly_forecast
from .selection import select_champions
from .types import Champion, Context

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
    models: int = 0


def _step_timer():
    """step(name) logs 'step=<name> <seconds since the previous step (or the timer's creation)>'."""
    from time import perf_counter
    last = [perf_counter()]

    def step(name: str) -> None:
        now = perf_counter()
        log.info("step=%s %.2fs", name, now - last[0])
        last[0] = now
    return step


def _fill_champion_wmape(champions: Dict[int, Champion], acc_periods: pd.DataFrame) -> None:
    """Final month-ahead=1 WMAPE per champion; wmape_default stays as selection set it."""
    def month1(frame: pd.DataFrame, k: int) -> Optional[float]:
        row = frame[(frame.kpi_id == k) & (frame.period == "month") & (frame.ahead == 1)]
        return float(row.wmape.iloc[0]) if len(row) and pd.notna(row.wmape.iloc[0]) else None
    for k, c in champions.items():
        c.wmape = month1(acc_periods, k)


def _log_champions(champions: Dict[int, Champion]) -> int:
    changed = 0
    for k, c in sorted(champions.items()):
        if c.reverted_for is not None:
            log.info("champion kpi=%s reverted for kpi=%s", k, c.reverted_for)
        if c.model == "swm" and c.calibration == 1.0:
            continue
        changed += 1
        log.info("champion kpi=%s model=%s w=%.2f cal=%.2f wmape=%.3f default=%.3f", k, c.model, c.w_swm,
                 c.calibration, c.wmape if c.wmape is not None else float("nan"),
                 c.wmape_default if c.wmape_default is not None else float("nan"))
    log.info("champions: %d of %d KPIs changed", changed, len(SELECTABLE_KPIS))
    return changed


def plan_year(origin: pd.Timestamp) -> int:
    """The planning year the bottle and club plans are read for."""
    return int(origin.year) + 1


def today_pacific() -> pd.Timestamp:
    now = datetime.now(PACIFIC)
    return pd.Timestamp(now.year, now.month, now.day)


def run(origin: Optional[pd.Timestamp] = None, horizon: Optional[int] = None, dry_run: bool = False,
        skip_backtest: bool = False, url: Optional[str] = None) -> RunSummary:
    step = _step_timer()
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
        # this year's plan still applies until year end; club._plan_rows filters by year
        plans = [fio.load_club_plan(conn, int(origin.year)), fio.load_club_plan(conn, py)]
        club_plan = pd.concat([p for p in plans if not p.empty] or plans[:1], ignore_index=True)
        tier_shares = fio.load_tier_shares(conn, origin, params.tier_min_members)
        label_plan = fio.load_label_plan(conn, py)
        vintage_plan = fio.load_vintage_plan(conn)
        hist_b = fio.load_bottle_history(conn, origin)
        memberships = fio.load_memberships(conn)
        snapshots = fio.load_bookings_snapshots(conn, origin - pd.Timedelta(days=params.backtest_days), origin)
    step("load")
    horizon = int(horizon or params.horizon)
    releases = detect_releases(hist[4], params)
    schedule = merge_schedule(parse_schedule(assumptions.get("shipment_schedule")), default_schedule(releases, origin))
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
    mark_planned(schedule, ctx.club_plan, a0, ctx.tier_shares)
    effective = {"engine": engine_params, "engine_version": ENGINE_VERSION,
                 "schedule": [{"start": s.start.strftime("%Y-%m-%d"), "days": s.days, "label": s.label,
                               "season": s.season, "planned_revenue": s.planned_revenue} for s in schedule],
                 "events": events, "members_a0": a0, "releases_detected": len(releases)}
    run_id: Optional[int] = None
    champions: Dict[int, Champion] = {}
    if not skip_backtest:
        _, bt_swm = fbt.run_backtest_full(hist, releases, params, origin, snapshots=snapshots)
        _, bt_level = fbt.run_backtest_full(hist, releases, params, origin,
                                            champions=fbt.uniform_champions(0.0), snapshots=snapshots)
        champions = select_champions(fbt.period_frame(bt_swm, params), fbt.period_frame(bt_level, params), params)
        step("select")
    effective["champions"] = {str(k): {"model": c.model, "w_swm": c.w_swm, "calibration": c.calibration}
                              for k, c in sorted(champions.items())}
    if not dry_run:
        with engine.begin() as conn:
            run_id = fio.start_run(conn, origin, horizon, effective)

    try:
        ctx.champions = champions
        point, comps = forecast_daily(hist, origin, dates, ctx)
        step("daily")
        if skip_backtest:
            accuracy, bt_frame = pd.DataFrame(columns=fbt.ACCURACY_COLUMNS), None
        else:
            accuracy, bt_frame = fbt.run_backtest_full(hist, releases, params, origin, champions=champions,
                                                       snapshots=snapshots)
        step("backtest")
        p10, p90 = fbt.apply_bands(point, accuracy, params)
        monthly = monthly_forecast(hist, origin, ctx, daily_point=point)
        step("monthly")
        if bt_frame is None:
            periods = pd.DataFrame(columns=fbt.PERIOD_COLUMNS)
            acc_periods = pd.DataFrame(columns=fbt.ACCURACY_PERIOD_COLUMNS)
        else:
            periods, acc_periods = fbt.run_period_backtest(bt_frame, params)
            _fill_champion_wmape(champions, acc_periods)
        step("periods")
        bottle_fc = bottle_summ = profiles = bt_v = None
        if hist_b.empty:
            log.warning("bottles skipped: no bottle history through %s", origin.date())
        else:
            vintages = vintage_table(hist_b, origin)
            bands = price_bands(hist_b)
            bt_v = backtest_vintages(hist_b, vintages, params)
            profiles = fit_profiles(hist_b, vintages, params, dispersion=bt_v)
            bottle_fc, bottle_summ = forecast_vintages(profiles, vintages, hist_b, ctx.label_plan, ctx.vintage_plan,
                                                       origin, plan_year(origin), params, bands)
        step("bottles")
        club_history = None
        if memberships.empty:
            log.warning("club history skipped: no memberships")
        else:
            recent = [r for r in releases if r.start > origin - pd.DateOffset(years=3)]
            with engine.connect() as conn:
                orders_by_release = {
                    r.start: fio.load_club_orders(conn, r.start - pd.Timedelta(days=1),
                                                  r.start + pd.Timedelta(days=r.days + 2))
                    for r in recent}
                members_daily = (fio.load_members_daily(conn, recent[0].start - pd.Timedelta(days=1), origin)
                                 if recent else None)
            tiers = tier_table(memberships, origin, params.tier_min_members)
            # compare with members_a0 on a dry run: the club tiers should account for most members
            log.info("club tiers=%s active_in_listed_tiers=%s members=%s",
                     len(tiers), int(tiers.active.sum()) if len(tiers) else 0, a0)
            club_history = release_history(orders_by_release, memberships, members_daily, tiers, params)
        step("clubs")

        next28 = float(point[14].iloc[:28].sum())
        for k in ALL_KPIS:
            row = accuracy[(accuracy.kpi_id == k) & (accuracy.horizon_bucket == "8-28")]
            wm = f"{float(row.wmape.iloc[0]):.3f}" if len(row) and pd.notna(row.wmape.iloc[0]) else "n/a"
            log.info("kpi=%s next28=%.2f wmape_8_28=%s", k, float(point[k].iloc[:28].sum()), wm)

        daily_recs = fio.daily_records(point, p10, p90, comps, run_id=0)
        monthly_recs = fio.monthly_records(monthly, run_id=0)
        acc_recs = fio.accuracy_records(accuracy, run_id=0)
        period_recs = fio.period_records(periods, run_id=0)
        acc_period_recs = fio.accuracy_period_records(acc_periods, run_id=0)
        bottle_recs = [] if bottle_fc is None else fio.bottle_records(bottle_fc, run_id=0)
        summ_recs = [] if bottle_summ is None else fio.vintage_summary_records(bottle_summ, run_id=0)
        profile_recs = [] if profiles is None else fio.profile_records(profiles, run_id=0)
        bt_v_recs = [] if bt_v is None else fio.bottle_backtest_records(bt_v, run_id=0)
        club_recs = [] if club_history is None else fio.club_history_records(club_history)
        wm1 = []
        for k in (4, 6, 14):
            row = acc_periods[(acc_periods.kpi_id == k) & (acc_periods.period == "month") & (acc_periods.ahead == 1)]
            wm1.append(f"{float(row.wmape.iloc[0]):.3f}" if len(row) and pd.notna(row.wmape.iloc[0]) else "n/a")
        _log_champions(champions)
        model_recs = fio.model_records(champions, run_id=0)
        log.info("month ahead=1 wmape: kpi4=%s kpi6=%s kpi14=%s", *wm1)
        log.info("periods=%s bottles=%s club_history=%s", len(period_recs), len(bottle_recs), len(club_recs))
        summary = RunSummary(run_id=None, origin=origin, horizon=horizon, kpis=len(point.columns),
                             daily_rows=len(daily_recs), monthly_rows=len(monthly_recs),
                             accuracy_rows=len(acc_recs), next28_total_revenue=next28,
                             period_rows=len(period_recs), bottle_rows=len(bottle_recs),
                             models=len(model_recs))
        if dry_run:
            log.info("dry run: %s", summary)
            return summary

        for rec in (daily_recs + monthly_recs + acc_recs + period_recs + acc_period_recs + bottle_recs
                    + summ_recs + profile_recs + bt_v_recs + model_recs):
            rec["run_id"] = run_id
        with engine.begin() as conn:
            fio.write_results(conn, run_id, daily_recs, monthly_recs, acc_recs)
            fio.write_periods(conn, run_id, period_recs, acc_period_recs)
            fio.write_bottles(conn, run_id, bottle_recs, summ_recs, profile_recs, bt_v_recs)
            fio.write_models(conn, run_id, model_recs)
            # not run-scoped: a skipped computation must not wipe it, and a backfill must not
            # replace today's history with the origin's view (same reasoning as the bookings snapshot)
            if club_history is not None and origin == live and not club_recs:
                log.warning("club history write skipped: no history rows; the existing table is kept")
            elif club_history is not None and origin == live:
                fio.write_club_history(conn, club_recs)
            elif club_history is not None:
                log.info("club history write skipped: backfill origin %s", origin.date())
            if origin == live:
                fio.write_bookings_snapshot(conn, origin + pd.Timedelta(days=1), otb)
            else:
                log.info("bookings snapshot skipped: backfill origin %s; the OTB table reflects today, not the origin",
                         origin.date())
            fio.finish_run(conn, run_id, "success", bottle_rows=len(bottle_recs))
            fio.prune_runs(conn)
        step("write")
    except Exception as exc:  # noqa: BLE001 - we want to record any failure
        if run_id is not None:
            with engine.begin() as conn:
                fio.finish_run(conn, run_id, "failed", str(exc))
        raise
    summary.run_id = run_id
    log.info("run %s complete: %s", run_id, summary)
    return summary
