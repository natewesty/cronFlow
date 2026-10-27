"""Wine club fees as a release-offset event (spec §A2)."""
from __future__ import annotations

from typing import List, Optional

import pandas as pd

from ..config import RELEASE_MATCH_WINDOW_DAYS, EngineParams
from ..types import Release, ScheduledRelease
from .club import _members_at, match_release

COVER_DAYS = 7   # a schedule entry within this many days of a historical release stands for it


def fee_component(y: pd.Series, releases: List[Release], schedule: List[ScheduledRelease],
                  members_hist: pd.Series, projected_stock: pd.Series,
                  dates: pd.DatetimeIndex, params: EngineParams,
                  window_days: int = 30, origin: Optional[pd.Timestamp] = None) -> pd.Series:
    """Fees land on pickup weekends some days after a release. For each scheduled release,
    copy the positive days within `window_days` of the matched earlier release (one whose
    whole fee window is in history, with members > 0 at its start), scaled by membership,
    to the same offsets after the scheduled start. No match: nothing.

    A historical release that started within `window_days` before `origin` (default: the
    last history date) and that no schedule entry covers is treated as an entry at its
    actual start, scaled by the history membership at that start."""
    dates = pd.DatetimeIndex(dates)
    out = pd.Series(0.0, index=dates)
    y = y.astype(float)
    if not len(y):
        return out
    last = y.index[-1]
    origin = pd.Timestamp(origin) if origin is not None else last
    entries = [(s.start, _members_at(projected_stock, s.start)) for s in schedule]
    for r in releases:
        if r.start > origin - pd.Timedelta(days=window_days) and r.start <= origin and not any(
                abs((s.start - r.start).days) <= COVER_DAYS for s in schedule):
            entries.append((r.start, _members_at(members_hist, r.start)))
    for start, members_now in entries:
        r = match_release(releases, start, params.release_match_years, RELEASE_MATCH_WINDOW_DAYS,
                          last, window_days, members_hist)
        if r is None:
            continue
        scale = members_now / _members_at(members_hist, r.start)
        win = y.loc[r.start: r.start + pd.Timedelta(days=window_days)]
        for d, amount in win[win > 0].items():
            when = start + (d - r.start)
            if when in out.index:
                out[when] += float(amount) * scale
    return out
