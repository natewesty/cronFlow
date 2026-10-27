"""Recent-level model (spec §5.1): trailing 28-day level x 56-day weekday
profile x a shrunk year-ago seasonal index."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..config import EngineParams


def level_forecast(y: pd.Series, origin: pd.Timestamp, dates: pd.DatetimeIndex,
                   params: EngineParams) -> pd.Series:
    """Point forecast for `dates` using history up to `origin` (inclusive)."""
    dates = pd.DatetimeIndex(dates)
    y = y.loc[:origin].astype(float)
    if len(y) == 0:
        return pd.Series(np.zeros(len(dates)), index=dates)
    day = pd.Timedelta(days=1)
    lvl = float(y.loc[origin - 27 * day:origin].mean())

    tail = y.loc[origin - 55 * day:origin]
    by_dow = tail.groupby(tail.index.dayofweek).mean().reindex(range(7))
    m = float(tail.mean())
    if np.isfinite(m) and m > 0:
        profile = (by_dow / m).fillna(1.0).to_numpy()
    else:
        profile = np.ones(7)

    r = y.rolling(28, min_periods=7).mean()
    lag = pd.Timedelta(days=364)
    num = r.reindex(dates - lag).to_numpy(dtype=float)
    den = float(r.get(origin - lag, np.nan))
    if np.isfinite(den) and den > 0:
        ratio = num / den
        idx = np.where(np.isfinite(ratio), np.clip(ratio, 0.4, 2.5), 1.0)
    else:
        idx = np.ones(len(dates))
    idx = 1.0 + params.lambda_shrink * (idx - 1.0)

    out = np.nan_to_num(lvl * profile[dates.dayofweek] * idx)
    return pd.Series(out, index=dates)
