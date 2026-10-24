"""Event-sensitive KPIs (spec §4.5): outlier capping plus an explicit event calendar."""
from __future__ import annotations

from typing import List, Optional

import pandas as pd


def cap_series(y: pd.Series, pct: float) -> pd.Series:
    """Clip nonzero days above the `pct` quantile of nonzero days."""
    y = y.astype(float)
    nz = y[y > 0]
    if nz.empty:
        return y.copy()
    return y.clip(upper=float(nz.quantile(pct)))


def event_component(events: Optional[List[dict]], field: str, dates: pd.DatetimeIndex) -> pd.Series:
    """Sum of `field` from calendar entries whose date falls inside `dates`."""
    out = pd.Series(0.0, index=pd.DatetimeIndex(dates))
    for e in events or []:
        try:
            d = pd.Timestamp(e.get("date"))
        except (ValueError, TypeError):
            continue
        if pd.isna(d) or d not in out.index:
            continue
        try:
            out[d] += float(e.get(field) or 0.0)
        except (TypeError, ValueError):
            continue
    return out
