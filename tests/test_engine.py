import numpy as np
import pandas as pd

from forecast.config import ALL_KPIS, EngineParams
from forecast.engine import forecast_daily
from forecast.identities import apply_identities
from forecast.models.club import default_schedule, detect_releases, release_magnitude
from forecast.types import Context, Release
from tests.synth import make_history

P = EngineParams()
ORIGIN = pd.Timestamp("2026-10-23")
DATES = pd.date_range("2026-10-24", periods=120)


def _ctx(df, events=None, otb=None, schedule=None):
    rel = detect_releases(df[4], P)
    return Context(params=P, releases=rel,
                   schedule=default_schedule(rel, ORIGIN) if schedule is None else schedule,
                   events=events or [], otb=otb, members_a0=float(df.loc[ORIGIN, 23]))


def test_output_shape_identities_and_no_nans():
    df = make_history()
    point, comps = forecast_daily(df, ORIGIN, DATES, _ctx(df))
    assert list(point.columns) == ALL_KPIS and point.index.equals(DATES)
    assert not point.isna().any().any()
    chk = apply_identities(point)
    for k in [3, 6, 11, 14, 17, 20, 21, 26, 27]:
        assert np.allclose(chk[k], point[k]), k
    assert set(comps) == set(point.columns) - {3, 6, 11, 14, 17, 20, 21, 26, 27}
    assert list(comps[4].columns) == ["organic", "release"]
    assert list(comps[25].columns) == ["organic", "spike"]
    assert "otb" in comps[16].columns


def test_club_release_lands_on_scheduled_days_with_right_magnitude():
    df = make_history(rpm=300.0)
    ctx = _ctx(df)
    point, comps = forecast_daily(df, ORIGIN, DATES, ctx)
    s = ctx.schedule[0]                                  # Nov 2026 release
    members = point.loc[s.start, 23]
    assert abs(comps[4].loc[s.start, "release"] - 0.3 * members * 300) / (members * 300) < 0.02
    assert comps[4].loc[s.start + pd.Timedelta(days=1), "release"] > comps[4].loc[s.start, "release"]
    off = s.start + pd.Timedelta(days=5)
    assert point.loc[off, 4] < 2000


def test_event_calendar_adds_to_event_kpis():
    df = make_history()
    d = DATES[10]
    events = [{"date": d.strftime("%Y-%m-%d"), "label": "Gala", "event_fees": 50000, "event_wine": 7000, "guests": 100}]
    base, _ = forecast_daily(df, ORIGIN, DATES, _ctx(df))
    point, comps = forecast_daily(df, ORIGIN, DATES, _ctx(df, events=events))
    assert point.loc[d, 9] - base.loc[d, 9] == 50000
    assert point.loc[d, 12] - base.loc[d, 12] == 7000
    assert point.loc[d, 19] - base.loc[d, 19] == 100
    assert point.loc[d, 11] - base.loc[d, 11] == 50000           # identity carries it


def test_otb_floor_applies_within_60_days_only():
    df = make_history()
    near, far = DATES[3], DATES[90]
    otb = pd.DataFrame({"reservation_date": [near, far], "attribution": ["Tasting Room", "Tasting Room"],
                        "reservations": [100, 100], "guests": [5000, 5000], "fees": [1.0, 1.0]})
    point, comps = forecast_daily(df, ORIGIN, DATES, _ctx(df, otb=otb))
    assert point.loc[near, 16] == 5000 and point.loc[near, 18] == 5000
    assert point.loc[far, 16] < 5000
    assert comps[16].loc[far, "otb"] == 5000                      # still reported


def test_membership_stock_is_running_balance_from_a0():
    df = make_history()
    ctx = _ctx(df)
    point, _ = forecast_daily(df, ORIGIN, DATES, ctx)
    expected = ctx.members_a0 + (point[24] - point[25]).cumsum()
    assert np.allclose(point[23], expected)


def test_recent_release_spike_carries_into_forecast():
    df = make_history()
    rel = detect_releases(df[4], P)
    origin = rel[-1].start + pd.Timedelta(days=3)         # 3 days after the Sep 2026 release
    dates = pd.date_range(origin + pd.Timedelta(days=1), periods=30)
    ctx = Context(params=P, releases=[r for r in rel if r.start <= origin],
                  schedule=default_schedule(rel, origin), events=[], otb=None,
                  members_a0=float(df.loc[origin, 23]))
    point, comps = forecast_daily(df, origin, dates, ctx)
    assert comps[25]["spike"].iloc[0] > 0
    assert comps[25]["spike"].iloc[0] > comps[25]["spike"].iloc[10]
    assert comps[25]["spike"].iloc[25] == 0
