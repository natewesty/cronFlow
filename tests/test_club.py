import numpy as np
import pytest
import pandas as pd

from forecast.config import EngineParams
from forecast.models.club import (default_schedule, mark_planned, plan_ratios,
                                  planned_release_revenue, detect_releases, match_release,
                                  release_component, release_magnitude,
                                  release_magnitude_pooled, residual_series,
                                  scheduled_release_values)
from forecast.types import Release, ScheduledRelease
from tests.synth import make_history, release_starts

P = EngineParams()
ORIGIN = pd.Timestamp("2026-10-23")


def test_detect_releases_finds_planted_two_day_releases():
    df = make_history()
    rel = detect_releases(df[4], P)
    assert [r.start for r in rel] == release_starts(df.index[0], df.index[-1])
    assert all(r.days == 2 for r in rel)
    assert all(len(r.day_totals) == 2 and r.day_totals[1] > r.day_totals[0] for r in rel)


def test_default_schedule_shifts_recent_releases_by_364_days():
    df = make_history()
    rel = detect_releases(df[4], P)
    sched = default_schedule(rel, ORIGIN, years=2)
    assert len(sched) == 8
    assert all(s.start > ORIGIN and s.days == 2 and s.start.dayofweek == 2 for s in sched)
    assert sched == sorted(sched, key=lambda s: s.start)
    assert sched[0].start == pd.Timestamp("2026-11-18")        # third Wed Nov 2025 + 364
    assert sched[0].start == rel[-4].start + pd.Timedelta(days=364)


def test_release_magnitude_recovers_rpm_and_shares():
    df = make_history(rpm=300.0)
    rel = detect_releases(df[4], P)
    rpm, shares = release_magnitude(df[4], rel, df[23], P.release_lookback_n)
    assert abs(rpm - 300.0) < 1.0
    assert np.allclose(shares, [0.3, 0.7], atol=0.005)


def test_residual_series_removes_release_days():
    df = make_history()
    rel = detect_releases(df[4], P)
    resid = residual_series(df[4], rel)
    for r in rel:
        assert (resid.loc[r.dates] < 1000).all()
    assert resid.index.equals(df.index)


def test_match_release_prefers_same_season_one_year_back():
    rel = [Release(start=pd.Timestamp(d), days=2, day_totals=[1, 1]) for d in ("2025-09-03", "2025-11-19", "2026-02-25", "2026-05-06", "2026-09-02")]
    m = match_release(rel, pd.Timestamp("2027-09-01"))
    assert m.start == pd.Timestamp("2026-09-02")
    assert match_release(rel, pd.Timestamp("2027-07-15")) is None


def test_match_release_two_years_back_when_last_year_missing():
    rel = [Release(start=pd.Timestamp("2025-05-07"), days=2, day_totals=[1, 1])]
    assert match_release(rel, pd.Timestamp("2027-05-05")).start == pd.Timestamp("2025-05-07")


def test_matched_magnitude_beats_pooled_on_seasonal_releases():
    df = make_history(season_mult={"spring": 1.3, "summer": 0.6, "fall": 1.4, "winter": 1.0})
    origin = pd.Timestamp("2026-08-15"); h = df.loc[:origin]
    rel = detect_releases(h[4], P)
    sched = default_schedule(rel, origin)
    s = next(x for x in sched if x.start.month == 9)
    stock = df[23]
    vals = scheduled_release_values(h[4], rel, h[23], stock, [s], P)
    actual = df[4].reindex(s.dates).sum()
    rpm, _ = release_magnitude_pooled(h[4], rel, h[23], P.release_lookback_n)
    pooled = rpm * stock.loc[s.start]
    assert abs(vals[s.start][0] - actual) / actual < 0.05
    assert abs(vals[s.start][0] - actual) < abs(pooled - actual)


def test_matched_release_falls_back_to_pooled():
    df = make_history(start="2026-01-01", end="2026-10-23")   # under a year: no match possible
    origin = pd.Timestamp("2026-10-23"); h = df.loc[:origin]
    rel = detect_releases(h[4], P)
    s = ScheduledRelease(start=pd.Timestamp("2026-11-18"), days=2, label="x", season="winter")
    vals = scheduled_release_values(h[4], rel, h[23], h[23], [s], P)
    total, shares = vals[s.start]
    assert total > 0 and np.isfinite(total) and abs(sum(shares) - 1) < 1e-9


def test_release_component_places_values():
    s = ScheduledRelease(start=pd.Timestamp("2027-02-24"), days=2, label="x")
    dates = pd.date_range("2027-02-20", periods=10)
    out = release_component([s], {s.start: (1000.0, [0.3, 0.7])}, dates)
    assert out.loc["2027-02-24"] == 300 and out.loc["2027-02-25"] == 700 and out.sum() == 1000


def test_release_component_partial_overlap():
    dates = pd.date_range("2027-02-25", periods=10)       # starts on release day 2
    s = ScheduledRelease(start=pd.Timestamp("2027-02-24"), days=2, label="x")
    comp = release_component([s], {s.start: (1000.0, [0.3, 0.7])}, dates)
    assert comp[pd.Timestamp("2027-02-25")] == 700
    assert comp.sum() == 700


def test_default_schedule_keeps_release_in_progress_at_origin():
    df = make_history()
    rel = detect_releases(df[4], P)
    origin = rel[-4].start + pd.Timedelta(days=364)       # day 1 of last year's shifted release
    sched = default_schedule(rel, origin)
    assert origin in [s.start for s in sched]


def test_release_magnitude_ignores_in_progress_release():
    df = make_history(rpm=300.0)
    rel = detect_releases(df[4], P)
    cut = df.loc[:rel[-1].start]
    rels = detect_releases(cut[4], P)
    assert rels[-1].start == rel[-1].start
    rpm, shares = release_magnitude(cut[4], rels, cut[23], P.release_lookback_n)
    assert abs(rpm - 300.0) < 1.0
    assert np.allclose(shares, [0.3, 0.7], atol=0.005)


def test_default_schedule_does_not_double_list_when_origin_is_release_day_one():
    from forecast.types import Release
    # Last year's release and this year's release (day 1 == origin) are both in the
    # trailing 365 days: last year's +2y shift lands on this year's +1y shift.
    last_year = Release(start=pd.Timestamp("2025-11-19"), days=2, day_totals=[1.0, 2.0])
    this_year = Release(start=pd.Timestamp("2026-11-18"), days=2, day_totals=[1.0, 2.0])
    origin = this_year.start
    sched = default_schedule([last_year, this_year], origin, years=2)
    starts = [s.start for s in sched]
    assert len(starts) == len(set(starts))
    assert starts == [pd.Timestamp("2026-11-18"), pd.Timestamp("2027-11-17"), pd.Timestamp("2028-11-15")]
    assert sched[1].label == "Nov 2026 +1y"          # keeps the first (nearest-year) listing


def test_default_schedule_drops_superseded_release_when_gap_is_357_days():
    from forecast.types import Release
    # Same seasonal release came 7 days earlier this year (357-day gap): last year's
    # +364 shift would put a phantom release a week after the real one.
    last_year = Release(start=pd.Timestamp("2025-09-10"), days=2, day_totals=[1.0, 2.0])
    this_year = Release(start=pd.Timestamp("2026-09-03"), days=2, day_totals=[1.0, 2.0])
    for origin in (pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-06")):
        sched = default_schedule([last_year, this_year], origin, years=2)
        starts = [s.start for s in sched]
        assert pd.Timestamp("2026-09-09") not in starts
        assert pd.Timestamp("2027-09-08") not in starts
        assert all(not s.label.startswith("Sep 2025") for s in sched)


def _plan(season, year, rows):
    return pd.DataFrame(rows, columns=["season", "release_date", "tier_id", "label", "vintage", "qty", "price", "take_rate", "members_override"]).assign(season=season, release_date=pd.Timestamp(f"{year}-09-01"))


def test_planned_release_revenue_members_times_take_times_box():
    plan = _plan("fall", 2027, [[None, None, "t1", "A", 2025, 2, 100.0, 0.5, None], [None, None, "t1", "B", 2025, 1, 50.0, 0.5, None], [None, None, "t2", "A", 2025, 4, 100.0, 0.8, 100]])
    shares = pd.DataFrame({"tier_id": ["t1", "t2"], "tier_name": ["T1", "T2"], "active": [900, 100], "share": [0.9, 0.1]})
    s = ScheduledRelease(start=pd.Timestamp("2027-09-01"), days=2, label="", season="fall")
    assert planned_release_revenue(plan, s, 1000.0, shares) == pytest.approx(144_500)


def test_planned_revenue_zero_is_ignored():
    plan = _plan("fall", 2027, [[None, None, "t1", "A", 2025, 0, 100.0, 0.5, None]])
    s = ScheduledRelease(start=pd.Timestamp("2027-09-01"), days=2, label="", season="fall")
    assert planned_release_revenue(plan, s, 1000.0, None) is None


def test_plan_ratios_scale_matched_total_to_plan():
    df = make_history(); origin = pd.Timestamp("2026-08-15"); h = df.loc[:origin]
    rel = detect_releases(h[4], P); sched = default_schedule(rel, origin)
    s = next(x for x in sched if x.start.month == 9)
    plan = _plan("fall", s.start.year, [[None, None, "t1", "A", 2024, 1, 1.0, 1.0, 500_000]])
    ratios = plan_ratios(h[4], rel, h[23], df[23], sched, P, plan, None)
    vals = scheduled_release_values(h[4], rel, h[23], df[23], [s], P, ratios)
    assert vals[s.start][0] == pytest.approx(500_000, rel=1e-6)
    assert set(ratios) == {s.start}
    mark_planned(sched, plan, 1000.0, None)
    assert s.planned_revenue == pytest.approx(500_000)
    assert all(x.planned_revenue is None for x in sched if x is not s)
