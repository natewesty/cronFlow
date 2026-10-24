"""Per-KPI daily forecast orchestration (spec §4.2–4.8). WS-A fills this in."""
from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd

from .types import Context


def forecast_daily(hist: pd.DataFrame, origin: pd.Timestamp, dates: pd.DatetimeIndex,
                   ctx: Context) -> Tuple[pd.DataFrame, Dict[int, pd.DataFrame]]:
    raise NotImplementedError("WS-A")
