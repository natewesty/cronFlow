"""On-the-books Tock reservations as a floor for near-term traffic/fee KPIs."""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def otb_series(otb: Optional[pd.DataFrame], field: str, attribution: Optional[str],
               dates: pd.DatetimeIndex) -> pd.Series:
    """Per-date sum of `field` (reservations|guests|fees) for one attribution
    (or all when None). NaN on dates with nothing on the books."""
    dates = pd.DatetimeIndex(dates)
    out = pd.Series(np.nan, index=dates, dtype=float)
    if otb is None or otb.empty:
        return out
    df = otb if attribution is None else otb[otb["attribution"] == attribution]
    if df.empty:
        return out
    s = df.groupby("reservation_date")[field].sum()
    s.index = pd.DatetimeIndex(s.index)
    return s.reindex(dates).astype(float)
