"""Shared constants and tunable parameters for the forecasting engine."""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field, fields
from typing import Dict, List, Optional, Tuple

import pandas as pd

KPI_SCHEMA = os.getenv("KPI_SCHEMA", "public_nate_sandbox")
FORECAST_SCHEMA = "forecast"
ENGINE_VERSION = "2.0.0"
RELEASE_MATCH_WINDOW_DAYS = 30
SEASONS = ("spring", "summer", "fall", "winter")
BOTTLE_CHANNELS = ("Club", "POS", "Web", "Inbound")

# --- KPI model families (spec §4.2) ---------------------------------------
SEASONAL_KPIS: List[int] = [1, 2, 7, 8, 15, 16, 18, 30, 31]
CLUB_KPIS: List[int] = [4, 13, 22, 28]
EVENT_KPIS: List[int] = [9, 10, 12, 19]
FEE_KPIS: List[int] = [5]
RATE_KPIS: List[int] = [29]
NEW_MEMBERS_KPI = 24
ATTRITION_KPI = 25
ACTIVE_MEMBERS_KPI = 23
MEMBERSHIP_KPIS: List[int] = [ACTIVE_MEMBERS_KPI, NEW_MEMBERS_KPI, ATTRITION_KPI]
MODELED_KPIS: List[int] = sorted(SEASONAL_KPIS + CLUB_KPIS + EVENT_KPIS + RATE_KPIS + MEMBERSHIP_KPIS + FEE_KPIS)

# --- Identities (spec §4.8, mart semantics) --------------------------------
SUM_IDENTITIES: Dict[int, List[int]] = {
    3: [1, 2],
    6: [4, 5],
    11: [9, 10],
    14: [1, 2, 4, 5, 7, 8, 9, 10, 12, 13],
}
DIFF_IDENTITIES: Dict[int, Tuple[int, int]] = {26: (24, 25)}
# kpi -> (numerator kpis, denominator kpis, scale)
RATIO_IDENTITIES: Dict[int, Tuple[List[int], List[int], float]] = {
    17: ([16], [15], 1.0),
    20: ([2, 5, 10], [16], 1.0),
    21: ([30], [18], 100.0),
    27: ([18], [24], 100.0),
}
DERIVED_KPIS: List[int] = sorted(list(SUM_IDENTITIES) + list(DIFF_IDENTITIES) + list(RATIO_IDENTITIES))
ALL_KPIS: List[int] = sorted(MODELED_KPIS + DERIVED_KPIS)

# KPIs allowed to go negative (refund-heavy). Others are floored at 0.
NEGATIVE_OK = {4, 13}

# On-the-books floors: kpi -> (otb field, attribution or None for all)
OTB_KPIS: Dict[int, Tuple[str, Optional[str]]] = {
    2: ("fees", "Tasting Room"),
    10: ("fees", "Event"),
    15: ("reservations", None),
    16: ("guests", None),
    18: ("guests", "Tasting Room"),
    19: ("guests", "Event"),
}

# Event calendar fields added on top of the capped seasonal forecast.
EVENT_CALENDAR_FIELD: Dict[int, str] = {9: "event_fees", 12: "event_wine", 19: "guests"}

# Growth family per KPI (used by monthly fitted_growth grouping and the app).
GROWTH_FAMILY: Dict[int, str] = {}
for _k in [1, 2, 3, 14, 15, 16, 17, 18, 20, 21, 22, 28, 29, 30, 31]:
    GROWTH_FAMILY[_k] = "tasting_room"
for _k in [4, 5, 6]:
    GROWTH_FAMILY[_k] = "club"
for _k in [7, 8, 13]:
    GROWTH_FAMILY[_k] = "ecomm_phone_ship"
for _k in [9, 10, 11, 12, 19]:
    GROWTH_FAMILY[_k] = "events"
for _k in [23, 24, 25, 26, 27]:
    GROWTH_FAMILY[_k] = "membership"
for _k in [32, 33]:
    GROWTH_FAMILY[_k] = "wholesale"

# Fixed-date holidays use calendar-date alignment instead of the weekday lag.
HOLIDAYS_FIXED = {(1, 1), (7, 4), (12, 24), (12, 25), (12, 31)}


def season_of(d: pd.Timestamp) -> str:
    """Club season of a date: Q1 spring, Q2 summer, Q3 fall, Q4 winter."""
    return SEASONS[(int(d.month) - 1) // 3]


@dataclass
class EngineParams:
    lambda_shrink: float = 0.7
    w1: float = 0.7
    w2: float = 0.3
    trend_window: int = 91
    trend_clip: Tuple[float, float] = (0.5, 2.0)
    horizon: int = 120
    otb_days: int = 60
    release_min_value: float = 50000.0
    release_median_mult: float = 5.0
    release_lookback_n: int = 4
    release_median_window: int = 90
    attrition_spike_days: int = 21
    event_cap_pct: float = 0.95
    backtest_days: int = 365
    backtest_step: int = 7
    horizon_buckets: List[Tuple[int, int]] = field(
        default_factory=lambda: [(1, 7), (8, 28), (29, 91), (92, 120)])
    monthly_months: int = 24
    monthly_lookback_months: int = 12
    period_weeks_ahead: int = 4
    period_months_ahead: int = 3
    bottle_curve_months: int = 36
    tier_min_members: int = 10
    release_match_years: int = 2

    @classmethod
    def from_overrides(cls, overrides: Optional[dict]) -> "EngineParams":
        allowed = {f.name for f in fields(cls)}
        clean = {k: v for k, v in (overrides or {}).items() if k in allowed}
        if "trend_clip" in clean:
            clean["trend_clip"] = tuple(clean["trend_clip"])
        if "horizon_buckets" in clean:
            clean["horizon_buckets"] = [tuple(b) for b in clean["horizon_buckets"]]
        return cls(**clean)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["trend_clip"] = list(self.trend_clip)
        d["horizon_buckets"] = [list(b) for b in self.horizon_buckets]
        return d
