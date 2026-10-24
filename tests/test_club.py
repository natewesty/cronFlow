import numpy as np
import pandas as pd

from forecast.config import EngineParams
from forecast.models.club import (default_schedule, detect_releases, release_component,
                                  release_magnitude, residual_series)
from forecast.types import ScheduledRelease
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


def test_release_component_places_rpm_times_members_times_share():
    dates = pd.date_range("2027-01-01", periods=60)
    members = pd.Series(3500.0, index=dates)
    sched = [ScheduledRelease(start=pd.Timestamp("2027-02-24"), days=2, label="x")]
    comp = release_component(sched, 300.0, [0.3, 0.7], members, dates)
    assert comp[pd.Timestamp("2027-02-24")] == 0.3 * 3500 * 300
    assert comp[pd.Timestamp("2027-02-25")] == 0.7 * 3500 * 300
    assert comp.sum() == 3500 * 300


def test_release_component_partial_overlap():
    dates = pd.date_range("2027-02-25", periods=10)       # starts on release day 2
    members = pd.Series(3500.0, index=dates)
    sched = [ScheduledRelease(start=pd.Timestamp("2027-02-24"), days=2, label="x")]
    comp = release_component(sched, 300.0, [0.3, 0.7], members, dates)
    assert comp[pd.Timestamp("2027-02-25")] == 0.7 * 3500 * 300
    assert comp.sum() == 0.7 * 3500 * 300
