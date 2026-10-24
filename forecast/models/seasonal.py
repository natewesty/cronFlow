"""Seasonal-weekday model (spec §4.3): same weekday one/two years back, scaled
by a shrunk trailing year-over-year trend factor."""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from ..config import HOLIDAYS_FIXED, NEGATIVE_OK, EngineParams


def _is_fixed_holiday(d: pd.Timestamp) -> bool:
    return (d.month, d.day) in HOLIDAYS_FIXED


def trend_factor(y: pd.Series, origin: pd.Timestamp, lag_days: int, params: EngineParams) -> float:
    """Shrunk ratio of the trailing `trend_window` days to the same window `lag_days` earlier."""
    y = y.loc[:origin].astype(float)
    start = origin - pd.Timedelta(days=params.trend_window - 1)
    lag = pd.Timedelta(days=lag_days)
    cur = float(y.loc[start:origin].sum())
    prev = float(y.loc[start - lag:origin - lag].sum())
    if not np.isfinite(prev) or prev <= 0 or not np.isfinite(cur) or cur <= 0:
        return 1.0
    lo, hi = params.trend_clip
    g = float(np.clip(cur / prev, lo, hi))
    return 1.0 + params.lambda_shrink * (g - 1.0)


def same_weekday_base(y: pd.Series, dates: pd.DatetimeIndex, lag_days: int) -> pd.Series:
    """Mean of the same-weekday observations at lag ±7 days; fixed-date holidays
    use the same calendar date `round(lag/364)` years back. NaN when unavailable."""
    dates = pd.DatetimeIndex(dates)
    y = y.astype(float)
    cols = [y.reindex(dates - pd.Timedelta(days=lag_days + off)).to_numpy(dtype=float)
            for off in (-7, 0, 7)]
    stack = np.column_stack(cols)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        base = np.nanmean(stack, axis=1)
    years = int(round(lag_days / 364))
    hol = np.array([_is_fixed_holiday(d) for d in dates], dtype=bool)
    if hol.any():
        hol_dates = pd.DatetimeIndex([d - pd.DateOffset(years=years) for d in dates[hol]])
        base[hol] = y.reindex(hol_dates).to_numpy(dtype=float)
    return pd.Series(base, index=dates)


def swm_forecast(y: pd.Series, origin: pd.Timestamp, dates: pd.DatetimeIndex,
                 params: EngineParams) -> pd.Series:
    """Point forecast for `dates` using history up to `origin` (inclusive)."""
    y = y.loc[:origin].astype(float)
    dates = pd.DatetimeIndex(dates)
    b1 = same_weekday_base(y, dates, 364).to_numpy()
    b2 = same_weekday_base(y, dates, 728).to_numpy()
    f1 = b1 * trend_factor(y, origin, 364, params)
    f2 = b2 * trend_factor(y, origin, 728, params)
    has1, has2 = np.isfinite(f1), np.isfinite(f2)
    out = np.where(has1 & has2, params.w1 * f1 + params.w2 * f2, np.where(has1, f1, f2))
    tail = y.loc[origin - pd.Timedelta(days=27):origin]
    fallback = float(tail.mean()) if len(tail) and np.isfinite(tail.mean()) else 0.0
    out = np.where(np.isfinite(out), out, fallback)
    return pd.Series(out, index=dates)


def apply_floor(fc: pd.Series, kpi_id: int, y: pd.Series) -> pd.Series:
    """Floor at 0, except refund-capable KPIs which floor at min(0, 5th percentile of history)."""
    if kpi_id in NEGATIVE_OK:
        vals = y.to_numpy(dtype=float)
        lo = float(np.nanpercentile(vals, 5)) if len(vals) else 0.0
        return fc.clip(lower=min(lo, 0.0))
    return fc.clip(lower=0.0)
