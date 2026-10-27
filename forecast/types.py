"""Dataclasses shared by the models, the runtime and the backtest."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

from .config import EngineParams


@dataclass
class Release:
    """A historical club release: consecutive billing days starting at `start`."""
    start: pd.Timestamp
    days: int
    day_totals: List[float]

    @property
    def total(self) -> float:
        return float(sum(self.day_totals))

    @property
    def dates(self) -> pd.DatetimeIndex:
        return pd.date_range(self.start, periods=self.days, freq="D")


@dataclass
class ScheduledRelease:
    """A future club release (default: historical release shifted +364 days)."""
    start: pd.Timestamp
    days: int
    label: str
    season: str = ""
    planned_revenue: Optional[float] = None

    @property
    def dates(self) -> pd.DatetimeIndex:
        return pd.date_range(self.start, periods=self.days, freq="D")


@dataclass
class Context:
    """Everything a forecast needs beyond the history itself."""
    params: EngineParams
    releases: List[Release]
    schedule: List[ScheduledRelease]
    events: List[dict] = field(default_factory=list)
    otb: Optional[pd.DataFrame] = None   # columns: reservation_date, attribution, reservations, guests, fees
    members_a0: float = 0.0
    club_plan: Optional[pd.DataFrame] = None     # season, release_date, tier_id, label, vintage, qty, price, take_rate, members_override
    tier_shares: Optional[pd.DataFrame] = None   # tier_id, tier_name, active, share
    label_plan: Optional[pd.DataFrame] = None    # plan_year, label, status, incoming_vintage, release_month, override_outgoing, override_incoming, price, varietal, comparable_label
    vintage_plan: Optional[pd.DataFrame] = None  # label, vintage, cases_made, assumed
