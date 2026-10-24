"""Coherence identities (spec §4.8) applied to wide KPI frames."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import (ACTIVE_MEMBERS_KPI, DIFF_IDENTITIES, RATE_KPIS,
                     RATIO_IDENTITIES, SUM_IDENTITIES)


def _safe_div(num: np.ndarray, den: np.ndarray) -> np.ndarray:
    num = np.asarray(num, dtype=float)
    den = np.asarray(den, dtype=float)
    out = np.zeros_like(num)
    mask = den != 0
    out[mask] = num[mask] / den[mask]
    return out


def apply_identities(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute every derived KPI column from its components. Missing
    component columns are treated as 0. Returns a copy with sorted columns."""
    out = df.copy()
    for k, parts in SUM_IDENTITIES.items():
        out[k] = sum(out[p] if p in out else 0.0 for p in parts)
    for k, (a, b) in DIFF_IDENTITIES.items():
        out[k] = (out[a] if a in out else 0.0) - (out[b] if b in out else 0.0)
    for k, (nums, dens, scale) in RATIO_IDENTITIES.items():
        n = sum(out[p] if p in out else 0.0 for p in nums)
        d = sum(out[p] if p in out else 0.0 for p in dens)
        out[k] = _safe_div(np.asarray(n), np.asarray(d)) * scale
    return out[sorted(out.columns)].fillna(0.0)


def aggregate_period(df: pd.DataFrame) -> pd.Series:
    """Collapse a wide frame to one value per KPI: sums for flows, last value
    for the membership stock, ratios recomputed from summed components, KPI 29
    averaged over days with orders (KPI 28 > 0)."""
    out = {}
    for k in df.columns:
        if k in RATIO_IDENTITIES:
            continue
        if k == ACTIVE_MEMBERS_KPI:
            out[k] = float(df[k].iloc[-1]) if len(df) else 0.0
        elif k in RATE_KPIS:
            mask = (df[28] > 0) if 28 in df else (df[k] > 0)
            out[k] = float(df.loc[mask, k].mean()) if mask.any() else 0.0
        else:
            out[k] = float(df[k].sum())
    for k, (nums, dens, scale) in RATIO_IDENTITIES.items():
        n = sum(out.get(p, 0.0) for p in nums)
        d = sum(out.get(p, 0.0) for p in dens)
        out[k] = (n / d * scale) if d else 0.0
    return pd.Series(out).sort_index()
