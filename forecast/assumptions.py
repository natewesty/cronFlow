"""Parse forecast.assumptions jsonb values into engine inputs (spec §4.7)."""
from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd

from .config import EngineParams, season_of
from .types import ScheduledRelease

log = logging.getLogger("forecast")


def parse_schedule(value) -> Optional[List[ScheduledRelease]]:
    if not isinstance(value, dict):
        return None
    out: List[ScheduledRelease] = []
    for item in value.get("releases") or []:
        if not isinstance(item, dict) or not item.get("start"):
            continue
        try:
            start = pd.Timestamp(item["start"]).normalize()
            days = int(item.get("days", 2))
        except (ValueError, TypeError, OverflowError, AttributeError):
            continue
        if pd.isna(start) or days < 1:
            continue
        out.append(ScheduledRelease(start=start, days=days, label=str(item.get("label") or ""),
                                 season=season_of(start)))
    return sorted(out, key=lambda s: s.start) or None


def merge_schedule(assumed: Optional[List[ScheduledRelease]],
                   default: List[ScheduledRelease]) -> List[ScheduledRelease]:
    """Assumption entries win for their (season, year); default entries for any other
    (season, year) are kept. Sorted by start."""
    assumed = list(assumed or [])
    taken = {(season_of(s.start), s.start.year) for s in assumed}
    kept = [s for s in default if (season_of(s.start), s.start.year) not in taken]
    return sorted(assumed + kept, key=lambda s: s.start)


def parse_events(value) -> List[dict]:
    if not isinstance(value, dict):
        return []
    out: List[dict] = []
    for item in value.get("events") or []:
        if not isinstance(item, dict) or not item.get("date"):
            continue
        try:
            d = pd.Timestamp(item["date"]).normalize()
        except (ValueError, TypeError, OverflowError, AttributeError):
            continue
        if pd.isna(d):
            continue

        def num(key: str) -> float:
            try:
                return float(item.get(key) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        out.append({"date": d.strftime("%Y-%m-%d"), "label": str(item.get("label") or ""),
                    "event_fees": num("event_fees"), "event_wine": num("event_wine"), "guests": num("guests")})
    return sorted(out, key=lambda e: e["date"])


def parse_params(value) -> EngineParams:
    try:
        return EngineParams.from_overrides(value if isinstance(value, dict) else None)
    except (TypeError, ValueError) as exc:
        log.warning("ignoring invalid engine_params overrides (%s); using defaults", exc)
        return EngineParams()
