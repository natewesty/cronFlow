"""Membership flows and stock (spec §4.6)."""
from __future__ import annotations

from typing import Callable, List

import numpy as np
import pandas as pd

from ..types import Release, ScheduledRelease


def _members_at(members: pd.Series, when: pd.Timestamp) -> float:
    upto = members.loc[:when]
    return float(upto.iloc[-1]) if len(upto) else 0.0


def spike_profile(attr: pd.Series, releases: List[Release], members: pd.Series,
                  window: int, n: int) -> np.ndarray:
    """Mean per-member excess attrition for day offsets 0..window-1 after the
    last `n` releases. Excess = max(attrition − pre-release 28-day median, 0)."""
    attr = attr.astype(float)
    prof = np.zeros(window)
    cnt = 0
    for r in [r for r in releases if r.start in attr.index][-n:]:
        pre = attr.loc[r.start - pd.Timedelta(days=28): r.start - pd.Timedelta(days=1)]
        base = float(pre.median()) if len(pre) else 0.0
        m = _members_at(members, r.start)
        if m <= 0:
            continue
        seg = attr.reindex(pd.date_range(r.start, periods=window, freq="D")).to_numpy(dtype=float)
        seg = np.where(np.isfinite(seg), seg, base)
        prof += np.maximum(seg - base, 0.0) / m
        cnt += 1
    return prof / cnt if cnt else prof


def attrition_base_series(attr: pd.Series, releases: List[Release], window: int) -> pd.Series:
    """Attrition with each post-release window blanked and linearly interpolated."""
    y = attr.astype(float).copy()
    for r in releases:
        y.loc[r.start: r.start + pd.Timedelta(days=window - 1)] = np.nan
    return y.interpolate(limit_direction="both").fillna(0.0)


def spike_component(schedule: List[ScheduledRelease], profile: np.ndarray,
                    members_at: Callable[[pd.Timestamp], float],
                    dates: pd.DatetimeIndex) -> pd.Series:
    """profile[j] × members(release start) on day start+j, for days inside `dates`."""
    out = pd.Series(0.0, index=pd.DatetimeIndex(dates))
    for s in schedule:
        m = float(members_at(s.start))
        for j, v in enumerate(profile):
            d = s.start + pd.Timedelta(days=j)
            if d in out.index:
                out[d] += float(v) * m
    return out


def project_stock(a0: float, new: pd.Series, attr: pd.Series) -> pd.Series:
    return float(a0) + (new.astype(float) - attr.astype(float)).cumsum()
