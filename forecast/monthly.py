"""Monthly component forecast for the Plan Builder (spec §4.9). WS-A fills this in."""
from __future__ import annotations

from typing import Optional

import pandas as pd

from .types import Context

MONTHLY_COLUMNS = ["month_start", "kpi_id", "is_actual", "organic", "organic_neutral",
                   "release", "event", "fitted_growth", "p50"]


def monthly_forecast(hist: pd.DataFrame, origin: pd.Timestamp, ctx: Context,
                     daily_point: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    raise NotImplementedError("WS-A")
