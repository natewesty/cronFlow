"""Club release detection, scheduling and magnitude (spec §4.4)."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..config import RELEASE_MATCH_WINDOW_DAYS, EngineParams, season_of
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
            out.append(ScheduledRelease(start=o.start, days=max(o.days, r.days), label=f"{o.start:%b %Y}", season=season_of(o.start)))
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
                out.append(ScheduledRelease(start=start, days=r.days, label=f"{r.start:%b %Y} +{k}y", season=season_of(start)))
    return sorted(out, key=lambda s: s.start)


def _members_at(members: pd.Series, when: pd.Timestamp) -> float:
    upto = members.loc[:when]
    if len(upto):
        return float(upto.iloc[-1])
    return float(members.iloc[0]) if len(members) else 0.0


def release_magnitude_pooled(y: pd.Series, releases: List[Release], members: pd.Series,
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


# Backward-compatible name: the engine itself now uses the matched/planned values below
# and calls release_magnitude_pooled directly for the fallback; kept for other importers.
release_magnitude = release_magnitude_pooled


def _valid_members(m: float) -> bool:
    return bool(np.isfinite(m)) and m > 0


def match_release(releases: List[Release], start: pd.Timestamp, years: int = 2,
                  window_days: int = 30, last: Optional[pd.Timestamp] = None,
                  tail_days: int = 0, members: Optional[pd.Series] = None) -> Optional[Release]:
    """The release k years (364k days) before `start`, smallest k first; within a k,
    same-season candidates beat others, then the nearest date.

    With `last` (the last history date) only complete releases count: the release must
    end before `last`, and `start + tail_days <= last` (fees need their whole window).
    With `members`, a candidate with zero or non-finite members at its start is skipped."""
    for k in range(1, years + 1):
        target = start - pd.Timedelta(days=364 * k)
        cands = [r for r in releases if abs((r.start - target).days) <= window_days]
        if last is not None:
            cands = [r for r in cands
                     if r.start + pd.Timedelta(days=r.days - 1) < last
                     and r.start + pd.Timedelta(days=tail_days) <= last]
        if members is not None:
            cands = [r for r in cands if _valid_members(_members_at(members, r.start))]
        if cands:
            season = season_of(start)
            return min(cands, key=lambda r: (season_of(r.start) != season,
                                             abs((r.start - target).days)))
    return None


def release_value(y: pd.Series, r: Release) -> float:
    return float(y.reindex(r.dates).fillna(0).sum())


def release_shares(y: pd.Series, r: Release) -> List[float]:
    vals = y.reindex(r.dates).fillna(0).to_numpy(dtype=float)
    tot = float(vals.sum())
    if tot <= 0:
        return [1.0]
    return [float(v / tot) for v in vals]


def scheduled_release_values(y: pd.Series, releases: List[Release], members_hist: pd.Series,
                             projected_stock: pd.Series, schedule: List[ScheduledRelease],
                             params: EngineParams,
                             ratios: Optional[Dict[pd.Timestamp, float]] = None,
                             planned: Optional[Dict[pd.Timestamp, float]] = None
                             ) -> Dict[pd.Timestamp, Tuple[float, List[float]]]:
    """Total and day shares per scheduled release: the matched earlier (complete) release
    scaled by membership, else the pooled revenue-per-member fallback. `ratios` scale the
    totals of planned releases; `planned` (KPI 4 only) replaces a total outright, so a
    planned release equals its plan even when there is no history to scale."""
    out: Dict[pd.Timestamp, Tuple[float, List[float]]] = {}
    last = y.index[-1] if len(y) else None
    for s in schedule:
        r = match_release(releases, s.start, params.release_match_years, RELEASE_MATCH_WINDOW_DAYS, last)
        m = _members_at(members_hist, r.start) if r is not None else 0.0
        if r is not None and _valid_members(m):
            total = release_value(y, r) * _members_at(projected_stock, s.start) / m
            shares = release_shares(y, r)
        else:
            rpm, shares = release_magnitude_pooled(y, releases, members_hist, params.release_lookback_n)
            total = rpm * _members_at(projected_stock, s.start)
        if ratios:
            total *= ratios.get(s.start, 1.0)
        if planned and s.start in planned:
            total = planned[s.start]
        out[s.start] = (float(total), shares)
    return out


def release_component(schedule: List[ScheduledRelease],
                      values: Dict[pd.Timestamp, Tuple[float, List[float]]],
                      dates: pd.DatetimeIndex) -> pd.Series:
    """total × day share, placed on scheduled days inside `dates`."""
    out = pd.Series(0.0, index=pd.DatetimeIndex(dates))
    for s in schedule:
        if s.start not in values:
            continue
        total, shares = values[s.start]
        for j in range(s.days):
            d = s.start + pd.Timedelta(days=j)
            share = shares[j] if j < len(shares) else 0.0
            if j == s.days - 1:
                share += float(sum(shares[s.days:]))   # a shorter release keeps the whole total
            if d in out.index:
                out[d] += total * share
    return out


def _plan_rows(club_plan: Optional[pd.DataFrame], s: ScheduledRelease,
               peers: Optional[List[ScheduledRelease]] = None) -> pd.DataFrame:
    """Plan rows for the season and year of `s`. With `peers` (the schedule), when several
    entries share that season and year only the one nearest the plan's release_date gets
    the rows (ties: the earlier start)."""
    if club_plan is None or len(club_plan) == 0:
        return pd.DataFrame()
    dates = pd.to_datetime(club_plan["release_date"])
    mask = (club_plan["season"] == season_of(s.start)) & (dates.dt.year == s.start.year)
    rows = club_plan[mask]
    if peers and len(rows):
        same = [o for o in peers if season_of(o.start) == season_of(s.start) and o.start.year == s.start.year]
        if len(same) > 1:
            plan_date = dates[mask].min()
            nearest = min(same, key=lambda o: (abs((o.start - plan_date).days), o.start))
            if nearest.start != s.start:
                return club_plan.iloc[0:0]
    return rows


def planned_release_revenue(club_plan: Optional[pd.DataFrame], s: ScheduledRelease,
                            members_total: float,
                            tier_shares: Optional[pd.DataFrame],
                            peers: Optional[List[ScheduledRelease]] = None) -> Optional[float]:
    """Σ_tier members × take_rate × Σ(qty × price) for the plan rows of this release;
    None when there are no rows or the sum is not positive."""
    rows = _plan_rows(club_plan, s, peers)
    if rows.empty:
        return None
    share_of: Dict[str, float] = {}
    if tier_shares is not None and len(tier_shares):
        share_of = {str(t): float(v) for t, v in zip(tier_shares["tier_id"], tier_shares["share"])}
    total = 0.0
    for tier_id, g in rows.groupby("tier_id", sort=False):
        ov = g["members_override"].iloc[0]
        members = float(ov) if pd.notna(ov) else float(members_total) * share_of.get(str(tier_id), 0.0)
        take = float(g["take_rate"].fillna(0.0).iloc[0])
        box = float((g["qty"].fillna(0.0) * g["price"].fillna(0.0)).sum())
        total += members * take * box
    return float(total) if total > 0 else None


def plan_totals(projected_stock: pd.Series, schedule: List[ScheduledRelease],
                club_plan: Optional[pd.DataFrame], tier_shares: Optional[pd.DataFrame],
                peers: Optional[List[ScheduledRelease]] = None) -> Dict[pd.Timestamp, float]:
    """Planned KPI 4 revenue per planned release in `schedule` (`peers`: the full
    schedule, for the one-plan-per-season rule; defaults to `schedule`)."""
    out: Dict[pd.Timestamp, float] = {}
    for s in schedule:
        rev = planned_release_revenue(club_plan, s, _members_at(projected_stock, s.start),
                                      tier_shares, peers if peers is not None else schedule)
        if rev is not None:
            out[s.start] = rev
    return out


def plan_ratios(y4: pd.Series, releases: List[Release], members_hist: pd.Series,
                projected_stock: pd.Series, schedule: List[ScheduledRelease],
                params: EngineParams, club_plan: Optional[pd.DataFrame],
                tier_shares: Optional[pd.DataFrame],
                peers: Optional[List[ScheduledRelease]] = None) -> Dict[pd.Timestamp, float]:
    """planned revenue / matched (or pooled) value, for planned releases only. With no
    base (≤ 0) the ratio is 1.0: KPI 4 then takes the plan directly via `plan_totals`."""
    out: Dict[pd.Timestamp, float] = {}
    revs = plan_totals(projected_stock, schedule, club_plan, tier_shares, peers)
    for s in schedule:
        if s.start not in revs:
            continue
        base = scheduled_release_values(y4, releases, members_hist, projected_stock, [s], params)[s.start][0]
        out[s.start] = float(revs[s.start] / base) if base > 0 else 1.0
    return out


def mark_planned(schedule: List[ScheduledRelease], club_plan: Optional[pd.DataFrame],
                 members_total: float, tier_shares: Optional[pd.DataFrame]) -> None:
    for s in schedule:
        s.planned_revenue = planned_release_revenue(club_plan, s, members_total, tier_shares, schedule)
