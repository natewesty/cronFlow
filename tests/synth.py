"""Synthetic daily KPI history with known seasonality, trend, club releases
and attrition spikes. Used by engine unit tests instead of warehouse data."""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from forecast.identities import apply_identities

WEEKLY = np.array([0.7, 0.6, 0.8, 0.9, 1.1, 1.6, 1.3])  # Mon..Sun multipliers


def release_starts(start: pd.Timestamp, end: pd.Timestamp) -> List[pd.Timestamp]:
    """Last Wed of Feb, first Wed of May, first Wed of Sep, third Wed of Nov."""
    out = []
    for y in range(start.year, end.year + 1):
        for month, which in ((2, "last"), (5, "first"), (9, "first"), (11, "third")):
            days = pd.date_range(f"{y}-{month:02d}-01", periods=31, freq="D")
            weds = [d for d in days if d.month == month and d.dayofweek == 2]
            d = {"first": weds[0], "third": weds[2], "last": weds[-1]}[which]
            if start <= d <= end:
                out.append(d)
    return sorted(out)


def make_history(start: str = "2023-07-01", end: str = "2026-10-23", seed: int = 0,
                 annual_growth: float = 0.05, members0: int = 3000,
                 rpm: float = 300.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, end, freq="D")
    n = len(idx)
    doy = idx.dayofyear.to_numpy()
    dow = idx.dayofweek.to_numpy()
    annual = 1 + 0.4 * np.sin(2 * np.pi * (doy - 100) / 365.25)
    trend = (1 + annual_growth) ** (np.arange(n) / 365.25)
    base = 10000 * annual * WEEKLY[dow] * trend

    def noise(s: float) -> np.ndarray:
        return np.clip(rng.normal(1.0, s, n), 0.05, None)

    df = pd.DataFrame(index=idx)
    df[1] = base * noise(0.1)
    df[2] = base * 0.4 * noise(0.1)
    df[16] = np.round(base / 100 * noise(0.1))
    df[18] = np.round(df[16] * 0.85)
    df[15] = np.maximum(np.round(df[16] / 4), 1)
    df[30] = np.round(df[18] * 0.6)
    df[31] = df[30] * 2.5
    df[5] = base * 0.02 * noise(0.3)
    df[7] = 600 * trend * noise(0.5)
    df[8] = 400 * trend * noise(0.8)
    df[9] = 0.0
    df[10] = base * 0.05 * noise(0.5)
    df[12] = 0.0
    df[19] = np.round(df[16] * 0.1)
    df[13] = 200 * trend * noise(0.5)
    df[29] = 250 * noise(0.1)
    df[24] = rng.poisson(4, n).astype(float)
    df[25] = rng.poisson(3, n).astype(float)
    df[4] = 0.0
    df[22] = df[31] / 12 + 2
    df[28] = df[30] + 5

    pos = {d: i for i, d in enumerate(idx)}
    starts = release_starts(idx[0], idx[-1])
    # Pass 1: attrition spikes after each release (magnitude from pre-spike members).
    members = members0 + np.cumsum(df[24].to_numpy() - df[25].to_numpy())
    for r in starts:
        m = members[pos[r]]
        for j in range(21):
            d = r + pd.Timedelta(days=j)
            if d in pos:
                df.loc[d, 25] += np.round(m * 0.004 * np.exp(-j / 5))
    # Final member stock, then pass 2: release revenue from the final stock so
    # that total / members on the release day recovers `rpm` exactly.
    members = members0 + np.cumsum(df[24].to_numpy() - df[25].to_numpy())
    df[23] = members.astype(float)
    for r in starts:
        m = members[pos[r]]
        for j, share in enumerate((0.3, 0.7)):
            d = r + pd.Timedelta(days=j)
            if d not in pos:
                continue
            df.loc[d, 4] += share * m * rpm
            df.loc[d, 22] += share * m * 0.4
            df.loc[d, 13] += share * m * 2.0
            df.loc[d, 28] += share * m * 0.9
    for k in [3, 6, 11, 14, 17, 20, 21, 26, 27]:
        df[k] = 0.0
    return apply_identities(df)
