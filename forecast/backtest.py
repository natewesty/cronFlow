"""Rolling-origin backtest and interval bands (spec §4.10)."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config import ACTIVE_MEMBERS_KPI, SELECTABLE_KPIS, EngineParams, season_of
from .engine import forecast_daily
from .identities import aggregate_period
from .types import Champion, Context, Release, ScheduledRelease

ACCURACY_COLUMNS = ["kpi_id", "horizon_bucket", "wmape", "bias", "ratio_p10", "ratio_p90",
                    "weekly_wmape", "monthly_wmape", "n"]

PERIOD_COLUMNS = ["kpi_id", "period", "ahead", "period_start", "origin_date", "actual", "forecast",
                  "p10", "p90", "calendar"]
ACCURACY_PERIOD_COLUMNS = ["kpi_id", "period", "ahead", "wmape", "bias", "hit_rate", "n", "mae"]
_PF_COLUMNS = ["kpi_id", "period", "ahead", "period_start", "origin_date", "actual", "forecast", "calendar"]
_BT_COLUMNS = ["origin", "kpi_id", "date", "horizon", "actual", "forecast", "calendar"]
_MIN_PERIOD_RATIOS = 5


def bucket_for(horizon: int, params: EngineParams) -> str:
    for lo, hi in params.horizon_buckets:
        if lo <= horizon <= hi:
            return f"{lo}-{hi}"
    lo, hi = params.horizon_buckets[-1]
    return f"{lo}-{hi}"


def _wmape(a: np.ndarray, f: np.ndarray) -> Optional[float]:
    den = float(np.abs(a).sum())
    return float(np.abs(a - f).sum() / den) if den > 0 else None


def _bias(a: np.ndarray, f: np.ndarray) -> Optional[float]:
    den = float(np.abs(a).sum())
    return float((f - a).sum() / den) if den > 0 else None


def uniform_champions(w: float) -> Dict[int, Champion]:
    """Every selectable KPI at the same SWM weight."""
    return {k: Champion(w_swm=float(w)) for k in SELECTABLE_KPIS}


def _origin_otb(snapshots: Optional[pd.DataFrame], o: pd.Timestamp) -> Optional[pd.DataFrame]:
    """Booking snapshot taken the morning after origin o, as that origin's on-the-books."""
    if snapshots is None or snapshots.empty:
        return None
    rows = snapshots[pd.to_datetime(snapshots["snapshot_date"]) == o + pd.Timedelta(days=1)]
    if rows.empty:
        return None
    return rows.drop(columns=["snapshot_date"]).reset_index(drop=True)


def backtest_frame(hist: pd.DataFrame, releases: List[Release], params: EngineParams,
                   origin: pd.Timestamp, champions: Optional[Dict[int, Champion]] = None,
                   snapshots: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Forecast from every past origin (step days apart) and pair with actuals.
    `calendar` marks origins that had a booking snapshot."""
    origins = []
    o = origin - pd.Timedelta(days=params.backtest_step)
    earliest = origin - pd.Timedelta(days=params.backtest_days)
    while o >= earliest:
        origins.append(o)
        o -= pd.Timedelta(days=params.backtest_step)
    frames = []
    for o in origins:
        n = min(params.horizon, (origin - o).days)
        dates = pd.date_range(o + pd.Timedelta(days=1), periods=n, freq="D")
        past = [r for r in releases if r.start <= o]
        future = [ScheduledRelease(start=r.start, days=r.days, label="hist", season=season_of(r.start)) for r in releases
                  if r.start + pd.Timedelta(days=r.days - 1) > o]
        a0 = float(hist.loc[o, ACTIVE_MEMBERS_KPI]) if ACTIVE_MEMBERS_KPI in hist and o in hist.index else 0.0
        otb = _origin_otb(snapshots, o)
        ctx = Context(params=params, releases=past, schedule=future, events=[], otb=otb, members_a0=a0,
                      champions=champions or {})
        point, _ = forecast_daily(hist, o, dates, ctx)
        actual = hist.reindex(dates)
        for k in point.columns:
            frames.append(pd.DataFrame({
                "origin": o, "kpi_id": int(k), "date": dates, "horizon": np.arange(1, n + 1),
                "actual": actual[k].to_numpy(dtype=float) if k in actual else np.nan,
                "forecast": point[k].to_numpy(dtype=float),
                "calendar": otb is not None,
            }))
    if not frames:
        return pd.DataFrame(columns=_BT_COLUMNS)
    bt = pd.concat(frames, ignore_index=True)
    return bt.dropna(subset=["actual"]).reset_index(drop=True)


def summarize(bt: pd.DataFrame, params: EngineParams) -> pd.DataFrame:
    if bt.empty:
        return pd.DataFrame(columns=ACCURACY_COLUMNS)
    bt = bt.copy()
    bt["bucket"] = bt["horizon"].map(lambda h: bucket_for(int(h), params))
    rows = []
    for (k, b), g in bt.groupby(["kpi_id", "bucket"]):
        a = g["actual"].to_numpy(dtype=float)
        f = g["forecast"].to_numpy(dtype=float)
        ok = (a > 0) & (f > 0)
        p10 = p90 = None
        if ok.sum() >= 10:
            ratios = np.clip(a[ok] / f[ok], 0.2, 5.0)
            p10, p90 = float(np.percentile(ratios, 10)), float(np.percentile(ratios, 90))
        weekly = monthly = None
        if b == "8-28":
            sub = g.assign(wk=g["date"].dt.to_period("W"), mo=g["date"].dt.to_period("M"))
            w = sub.groupby(["origin", "wk"])[["actual", "forecast"]].sum()
            weekly = _wmape(w["actual"].to_numpy(), w["forecast"].to_numpy())
            mth = sub.groupby(["origin", "mo"])[["actual", "forecast"]].sum()
            monthly = _wmape(mth["actual"].to_numpy(), mth["forecast"].to_numpy())
        rows.append({"kpi_id": int(k), "horizon_bucket": b, "wmape": _wmape(a, f), "bias": _bias(a, f),
                     "ratio_p10": p10, "ratio_p90": p90, "weekly_wmape": weekly,
                     "monthly_wmape": monthly, "n": int(len(g))})
    out = pd.DataFrame(rows, columns=ACCURACY_COLUMNS)
    return out.astype({"wmape": float, "bias": float, "ratio_p10": float, "ratio_p90": float,
                       "weekly_wmape": float, "monthly_wmape": float})


def run_backtest_full(hist: pd.DataFrame, releases: List[Release], params: EngineParams,
                      origin: pd.Timestamp, champions: Optional[Dict[int, Champion]] = None,
                      snapshots: Optional[pd.DataFrame] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Daily accuracy summary plus the backtest frame it came from (for the period backtest)."""
    bt = backtest_frame(hist, releases, params, origin, champions, snapshots)
    return summarize(bt, params), bt


def run_backtest(hist: pd.DataFrame, releases: List[Release], params: EngineParams,
                 origin: pd.Timestamp, champions: Optional[Dict[int, Champion]] = None,
                 snapshots: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    return run_backtest_full(hist, releases, params, origin, champions, snapshots)[0]


def apply_bands(point: pd.DataFrame, accuracy: pd.DataFrame,
                params: EngineParams) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """p10/p90 = p50 × the KPI's ratio band for the day's horizon bucket."""
    horizons = np.arange(1, len(point.index) + 1)
    buckets = np.array([bucket_for(int(h), params) for h in horizons])
    p10 = pd.DataFrame(np.nan, index=point.index, columns=point.columns)
    p90 = pd.DataFrame(np.nan, index=point.index, columns=point.columns)
    if accuracy is None or accuracy.empty:
        return p10, p90
    lookup = {(int(r.kpi_id), r.horizon_bucket): (r.ratio_p10, r.ratio_p90) for r in accuracy.itertuples()}
    for k in point.columns:
        lo = np.array([lookup.get((int(k), b), (np.nan, np.nan))[0] for b in buckets], dtype=float)
        hi = np.array([lookup.get((int(k), b), (np.nan, np.nan))[1] for b in buckets], dtype=float)
        v = point[k].to_numpy(dtype=float)
        a, b = v * lo, v * hi
        # A zero forecast has a zero band whatever the ratio (none is finite when the
        # backtest forecast was always 0), so identity sums over it keep their bands.
        zero = v == 0
        p10[k] = np.where(zero, 0.0, np.minimum(a, b))
        p90[k] = np.where(zero, 0.0, np.maximum(a, b))
    return p10, p90


def _period_keys(dates: pd.DatetimeIndex, period: str) -> pd.Series:
    if period == "week":
        return pd.Series(dates.to_period("W-SUN").start_time, index=dates)
    return pd.Series(dates.to_period("M").start_time, index=dates)


def period_frame(bt: pd.DataFrame, params: EngineParams) -> pd.DataFrame:
    """Aggregate each origin's forecast and actuals to full weeks (Mon-Sun) and calendar
    months lying wholly inside that origin's forecast window; one row per kpi/period/ahead
    from the latest origin."""
    if bt.empty:
        return pd.DataFrame(columns=_PF_COLUMNS)
    limits = {"week": params.period_weeks_ahead, "month": params.period_months_ahead}
    rows = []
    for origin, g in bt.groupby("origin"):
        act = g.pivot(index="date", columns="kpi_id", values="actual").sort_index()
        fc = g.pivot(index="date", columns="kpi_id", values="forecast").sort_index()
        have = set(act.index)
        cal = bool(g["calendar"].iloc[0]) if "calendar" in g else False
        window = set(pd.date_range(origin + pd.Timedelta(days=1), periods=params.horizon, freq="D"))
        for period, limit in limits.items():
            keys = _period_keys(act.index, period)
            ahead = 0
            for start in sorted(keys.unique()):
                days = act.index[keys.to_numpy() == start]
                if period == "week":
                    end = start + pd.Timedelta(days=6)
                else:
                    end = start + pd.offsets.MonthEnd(0)
                full = pd.date_range(start, end, freq="D")
                if not all(d in have and d in window for d in full):
                    continue
                ahead += 1
                if ahead > limit:
                    break
                a = aggregate_period(act.loc[days])
                f = aggregate_period(fc.loc[days])
                for k in a.index:
                    rows.append((int(k), period, ahead, pd.Timestamp(start), pd.Timestamp(origin),
                                 float(a[k]), float(f[k]), cal))
    out = pd.DataFrame(rows, columns=_PF_COLUMNS)
    if out.empty:
        return out
    out = out.sort_values("origin_date").drop_duplicates(
        ["kpi_id", "period", "period_start", "ahead"], keep="last")
    return out.sort_values(["kpi_id", "period", "ahead", "period_start"]).reset_index(drop=True)


def summarize_periods(pf: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Period-level ratio bands (p10/p90 of actual/forecast per kpi/period/ahead), hit rate
    and accuracy."""
    if pf.empty:
        return pd.DataFrame(columns=PERIOD_COLUMNS), pd.DataFrame(columns=ACCURACY_PERIOD_COLUMNS)
    periods, acc = [], []
    if "calendar" not in pf:
        pf = pf.assign(calendar=False)
    for (k, period, ahead), g in pf.groupby(["kpi_id", "period", "ahead"]):
        a = g["actual"].to_numpy(dtype=float)
        f = g["forecast"].to_numpy(dtype=float)
        ok = (a > 0) & (f > 0)
        lo = hi = np.nan
        if ok.sum() >= _MIN_PERIOD_RATIOS:
            lo, hi = np.percentile(a[ok] / f[ok], [10, 90])
        b1, b2 = f * lo, f * hi
        p10, p90 = np.minimum(b1, b2), np.maximum(b1, b2)
        part = g.copy()
        part["p10"], part["p90"] = p10, p90
        periods.append(part)
        # A period where nothing happened and nothing was forecast is not a hit.
        keep = ~((a == 0) & (f == 0))
        ak, fk = a[keep], f[keep]
        hit = (float(np.mean((ak >= p10[keep]) & (ak <= p90[keep])))
               if keep.any() and not np.isnan(lo) else np.nan)
        w, b = _wmape(ak, fk), _bias(ak, fk)
        acc.append({"kpi_id": int(k), "period": period, "ahead": int(ahead),
                    "wmape": np.nan if w is None else w, "bias": np.nan if b is None else b,
                    "hit_rate": hit, "n": int(keep.sum()),
                    "mae": float(np.abs(ak - fk).mean()) if keep.any() else np.nan})
    pdf = pd.concat(periods, ignore_index=True)[PERIOD_COLUMNS]
    return pdf, pd.DataFrame(acc, columns=ACCURACY_PERIOD_COLUMNS)


def run_period_backtest(bt: pd.DataFrame, params: EngineParams) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return summarize_periods(period_frame(bt, params))
