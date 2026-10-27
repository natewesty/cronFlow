"""Wine club fees as a release-offset event (spec §A2)."""
from __future__ import annotations

from typing import List

import pandas as pd

from ..config import RELEASE_MATCH_WINDOW_DAYS, EngineParams
from ..types import Release, ScheduledRelease
from .club import _members_at, match_release


def fee_component(y: pd.Series, releases: List[Release], schedule: List[ScheduledRelease],
                  members_hist: pd.Series, projected_stock: pd.Series,
                  dates: pd.DatetimeIndex, params: EngineParams,
                  window_days: int = 30) -> pd.Series:
    """Fees land on pickup weekends some days after a release. For each scheduled release,
    copy the non-zero days within `window_days` of the matched earlier release, scaled by
    membership, to the same offsets after the scheduled start. No match: nothing."""
    dates = pd.DatetimeIndex(dates)
    out = pd.Series(0.0, index=dates)
    y = y.astype(float)
    for s in schedule:
        r = match_release(releases, s.start, params.release_match_years, RELEASE_MATCH_WINDOW_DAYS)
        if r is None:
            continue
        scale = _members_at(projected_stock, s.start) / max(_members_at(members_hist, r.start), 1)
        win = y.loc[r.start: r.start + pd.Timedelta(days=window_days)]
        for d, amount in win[win != 0].items():
            when = s.start + (d - r.start)
            if when in out.index:
                out[when] += float(amount) * scale
    return out
