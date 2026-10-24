"""Rolling-origin backtest and interval bands (spec §4.10). WS-A fills this in."""
from __future__ import annotations

from typing import List

import pandas as pd

from .config import EngineParams
from .types import Release

ACCURACY_COLUMNS = ["kpi_id", "horizon_bucket", "wmape", "bias", "ratio_p10", "ratio_p90",
                    "weekly_wmape", "monthly_wmape", "n"]


def bucket_for(horizon: int, params: EngineParams) -> str:
    for lo, hi in params.horizon_buckets:
        if lo <= horizon <= hi:
            return f"{lo}-{hi}"
    lo, hi = params.horizon_buckets[-1]
    return f"{lo}-{hi}"


def run_backtest(hist: pd.DataFrame, releases: List[Release], params: EngineParams,
                 origin: pd.Timestamp) -> pd.DataFrame:
    raise NotImplementedError("WS-A")
