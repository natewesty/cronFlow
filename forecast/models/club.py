"""Club release detection, scheduling and magnitude (spec §4.4)."""
from __future__ import annotations

from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from ..config import EngineParams
from ..types import Release, ScheduledRelease


def detect_releases(club: pd.Series, params: EngineParams) -> List[Release]:
    """Release day: value > release_min_value and > release_median_mult × the
    trailing-90-day median (excluding the day itself). Consecutive release days
    form one release."""
    club = club.astype(float)
    med = (club.rolling(params.release_median_window, min_periods=30).median()
           .shift(1).fillna(0.0))
    flag = (club > params.release_min_value) & (club > params.release_median_mult * med)
    releases: List[Release] = []
    cur: Optional[Release] = None
    for d, f in flag.items():
        if not f:
            cur = None
            continue
        if cur is not None and d == cur.start + pd.Timedelta(days=cur.days):
            cur.days += 1
            cur.day_totals.append(float(club[d]))
        else:
            cur = Release(start=d, days=1, day_totals=[float(club[d])])
            releases.append(cur)
    return releases


def default_schedule(releases: List[Release], origin: pd.Timestamp, years: int = 2) -> List[ScheduledRelease]:
    """Each release in the 365 days before origin, shifted +364 days per year ahead."""
    out: List[ScheduledRelease] = []
    seen = set()
    recent = [r for r in releases if r.start > origin - pd.Timedelta(days=365)]
    # Keep the latest release per yearly slot: a recent release with a later one 300-430
    # days after it is the same seasonal event, superseded (its shift would be a phantom).
    kept: List[Release] = []
    for r in recent:
        later = [o for o in releases if pd.Timedelta(days=300) <= o.start - r.start <= pd.Timedelta(days=430)]
        if not later:
            kept.append(r)
            continue
        # The superseding release may be truncated at origin and still running: schedule it
        # as itself, with the predecessor's length when that is longer.
        o = later[-1]
        if o.start + pd.Timedelta(days=o.days - 1) >= origin and o.start <= origin:
            seen.add(o.start)
            out.append(ScheduledRelease(start=o.start, days=max(o.days, r.days), label=f"{o.start:%b %Y}"))
    recent = kept
    for k in range(1, years + 1):
        for r in recent:
            start = r.start + pd.Timedelta(days=364 * k)
            # Two releases 364 days apart (e.g. origin on a release's first day) shift
            # onto the same start; keep the first (nearest-year) listing only.
            if start in seen:
                continue
            if start + pd.Timedelta(days=r.days - 1) > origin:
                seen.add(start)
                out.append(ScheduledRelease(start=start, days=r.days, label=f"{r.start:%b %Y} +{k}y"))
    return sorted(out, key=lambda s: s.start)


def _members_at(members: pd.Series, when: pd.Timestamp) -> float:
    upto = members.loc[:when]
    if len(upto):
        return float(upto.iloc[-1])
    return float(members.iloc[0]) if len(members) else 0.0


def release_magnitude(y: pd.Series, releases: List[Release], members: pd.Series,
                      n: int) -> Tuple[float, List[float]]:
    """Mean revenue-per-member over the last `n` releases and the mean per-day share."""
    if not len(y):
        return 0.0, [1.0]
    last = y.index[-1]
    rs = [r for r in releases
          if r.start in y.index and r.start + pd.Timedelta(days=r.days - 1) < last][-n:]
    if not rs:
        return 0.0, [1.0]
    rpms, shares = [], []
    for r in rs:
        vals = y.reindex(r.dates).fillna(0.0).to_numpy(dtype=float)
        m = _members_at(members, r.start)
        tot = float(vals.sum())
        if m > 0:
            rpms.append(tot / m)
        if tot > 0:
            shares.append(vals / tot)
    rpm = float(np.mean(rpms)) if rpms else 0.0
    if not shares:
        return rpm, [1.0]
    width = max(len(s) for s in shares)
    padded = np.array([np.pad(s, (0, width - len(s))) for s in shares])
    share = padded.mean(axis=0)
    if share.sum() > 0:
        share = share / share.sum()
    return rpm, [float(x) for x in share]


def residual_series(y: pd.Series, releases: List[Release], pad: int = 14) -> pd.Series:
    """History with release days replaced by the median of the surrounding ±pad days."""
    y = y.astype(float).copy()
    for r in releases:
        win = y.loc[r.start - pd.Timedelta(days=pad): r.start + pd.Timedelta(days=r.days - 1 + pad)]
        rest = win.drop(r.dates, errors="ignore")
        y.loc[y.index.isin(r.dates)] = float(rest.median()) if len(rest) else 0.0
    return y


def release_component(schedule: List[ScheduledRelease], rpm: float, shares: List[float],
                      projected_members: pd.Series, dates: pd.DatetimeIndex) -> pd.Series:
    """rpm × members(at release start) × day share, placed on scheduled days inside `dates`."""
    out = pd.Series(0.0, index=pd.DatetimeIndex(dates))
    for s in schedule:
        m = _members_at(projected_members, s.start)
        for j in range(s.days):
            d = s.start + pd.Timedelta(days=j)
            share = shares[j] if j < len(shares) else 0.0
            if d in out.index:
                out[d] += rpm * m * share
    return out
