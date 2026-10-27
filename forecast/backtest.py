"""Rolling-origin backtest and interval bands (spec §4.10)."""
from __future__ import annotations

from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .config import ACTIVE_MEMBERS_KPI, EngineParams, season_of
from .engine import forecast_daily
from .types import Context, Release, ScheduledRelease

ACCURACY_COLUMNS = ["kpi_id", "horizon_bucket", "wmape", "bias", "ratio_p10", "ratio_p90",
                    "weekly_wmape", "monthly_wmape", "n"]


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


def backtest_frame(hist: pd.DataFrame, releases: List[Release], params: EngineParams,
                   origin: pd.Timestamp) -> pd.DataFrame:
    """Forecast from every past origin (step days apart) and pair with actuals."""
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
        ctx = Context(params=params, releases=past, schedule=future, events=[], otb=None, members_a0=a0)
        point, _ = forecast_daily(hist, o, dates, ctx)
        actual = hist.reindex(dates)
        for k in point.columns:
            frames.append(pd.DataFrame({
                "origin": o, "kpi_id": int(k), "date": dates, "horizon": np.arange(1, n + 1),
                "actual": actual[k].to_numpy(dtype=float) if k in actual else np.nan,
                "forecast": point[k].to_numpy(dtype=float),
            }))
    if not frames:
        return pd.DataFrame(columns=["origin", "kpi_id", "date", "horizon", "actual", "forecast"])
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


def run_backtest(hist: pd.DataFrame, releases: List[Release], params: EngineParams,
                 origin: pd.Timestamp) -> pd.DataFrame:
    return summarize(backtest_frame(hist, releases, params, origin), params)


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
