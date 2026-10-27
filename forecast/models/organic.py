"""Organic forecast: champion blend of the seasonal and recent-level models."""
from __future__ import annotations

from typing import Dict

import pandas as pd

from ..config import SELECTABLE_KPIS, EngineParams
from ..types import Champion
from .level import level_forecast
from .seasonal import swm_forecast


def organic_forecast(k: int, y: pd.Series, origin: pd.Timestamp, dates: pd.DatetimeIndex,
                     params: EngineParams, champions: Dict[int, Champion]) -> pd.Series:
    """m * (w*swm + (1-w)*level) for k's champion; plain SWM without one."""
    champ = champions.get(k) if k in SELECTABLE_KPIS else None
    if champ is None:
        return swm_forecast(y, origin, dates, params)
    w, m = float(champ.w_swm), float(champ.calibration)
    if w >= 1.0:
        base = swm_forecast(y, origin, dates, params)
    elif w <= 0.0:
        base = level_forecast(y, origin, dates, params)
    else:
        base = w * swm_forecast(y, origin, dates, params) + (1.0 - w) * level_forecast(y, origin, dates, params)
    return base if m == 1.0 else m * base
