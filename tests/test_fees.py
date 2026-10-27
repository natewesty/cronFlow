import pandas as pd
import pytest

from forecast.config import EngineParams
from forecast.models.fees import fee_component
from forecast.types import Release, ScheduledRelease

P = EngineParams()


def test_fee_component_places_amounts_at_matched_offsets():
    idx = pd.date_range("2025-01-01", "2026-10-25")
    y = pd.Series(0.0, index=idx); y["2026-05-16"] = 36226; y["2026-05-17"] = 28994
    rel = [Release(start=pd.Timestamp("2026-05-06"), days=2, day_totals=[1, 1])]
    s = ScheduledRelease(start=pd.Timestamp("2027-05-05"), days=2, label="", season="summer")
    members = pd.Series(1000.0, index=idx); stock = pd.Series(1100.0, index=pd.date_range("2026-10-26", "2027-12-31"))
    dates = pd.date_range("2027-04-01", "2027-06-30")
    out = fee_component(y, rel, [s], members, stock, dates, P)
    assert out.loc["2027-05-15"] == pytest.approx(36226 * 1.1) and out.loc["2027-05-16"] == pytest.approx(28994 * 1.1)
    assert (out != 0).sum() == 2


def test_fee_component_no_match_is_zero():
    idx = pd.date_range("2026-01-01", "2026-10-25"); y = pd.Series(0.0, index=idx)
    s = ScheduledRelease(start=pd.Timestamp("2027-05-05"), days=2, label="", season="summer")
    out = fee_component(y, [], [s], pd.Series(1.0, index=idx), pd.Series(1.0, index=idx), pd.date_range("2027-05-01", periods=30), P)
    assert out.sum() == 0


def test_fee_component_does_not_copy_refund_days():
    idx = pd.date_range("2025-01-01", "2026-10-25")
    y = pd.Series(0.0, index=idx); y["2026-05-16"] = 1000; y["2026-05-18"] = -500
    rel = [Release(start=pd.Timestamp("2026-05-06"), days=2, day_totals=[1, 1])]
    s = ScheduledRelease(start=pd.Timestamp("2027-05-05"), days=2, label="", season="summer")
    one = pd.Series(1.0, index=idx)
    out = fee_component(y, rel, [s], one, one, pd.date_range("2027-04-01", "2027-06-30"), P)
    assert out.loc["2027-05-15"] == 1000 and out.loc["2027-05-17"] == 0 and (out < 0).sum() == 0


def test_fee_component_skips_match_with_zero_members_and_tries_next_year():
    idx = pd.date_range("2024-01-01", "2026-10-25")
    y = pd.Series(0.0, index=idx); y["2026-05-16"] = 1000; y["2025-05-17"] = 700
    rel = [Release(start=pd.Timestamp("2025-05-07"), days=2, day_totals=[1, 1]),
           Release(start=pd.Timestamp("2026-05-06"), days=2, day_totals=[1, 1])]
    members = pd.Series(100.0, index=idx); members["2026-05-06"] = 0.0
    s = ScheduledRelease(start=pd.Timestamp("2027-05-05"), days=2, label="", season="summer")
    stock = pd.Series(100.0, index=pd.date_range("2026-10-26", "2027-12-31"))
    out = fee_component(y, rel, [s], members, stock, pd.date_range("2027-04-01", "2027-06-30"), P)
    assert out.loc["2027-05-15"] == pytest.approx(700) and (out != 0).sum() == 1


def _fee_history():
    from forecast.models.club import detect_releases
    from tests.synth import make_history
    df = make_history(); rel = detect_releases(df[4], P)
    df[5] = 0.0
    for r in rel:
        d = r.start + pd.Timedelta(days=10)
        if d in df.index:
            df.loc[d, 5] = df.loc[r.start, 23] * 20
    return df, rel


def _fee_ctx(h, origin):
    from forecast.models.club import default_schedule, detect_releases
    from forecast.types import Context
    rel = detect_releases(h[4], P)
    return Context(params=P, releases=rel, schedule=default_schedule(rel, origin), members_a0=float(h[23].iloc[-1]))


def test_monthly_next_year_fee_uses_complete_release_window():
    from forecast.monthly import monthly_forecast
    df, rel = _fee_history(); r_last = rel[-1]
    origin = r_last.start + pd.Timedelta(days=5); h = df.loc[:origin]
    m = monthly_forecast(h, origin, _fee_ctx(h, origin))
    nxt = r_last.start + pd.Timedelta(days=364)
    row = m[(m.kpi_id == 5) & (m.month_start == (nxt + pd.Timedelta(days=10)).replace(day=1))].iloc[0]
    planted = df.loc[r_last.start + pd.Timedelta(days=10), 5]
    assert row.p50 > 0.5 * planted


def test_monthly_next_year_club_release_from_origin_on_release_day_one():
    from forecast.monthly import monthly_forecast
    df, rel = _fee_history(); r_last = rel[-1]
    origin = r_last.start; h = df.loc[:origin]
    m = monthly_forecast(h, origin, _fee_ctx(h, origin))
    month = (r_last.start + pd.Timedelta(days=364)).replace(day=1)
    row = m[(m.kpi_id == 4) & (m.month_start == month)].iloc[0]
    planted = df[4].reindex(r_last.dates).sum()
    assert abs(row.release - planted) / planted < 0.06


def test_daily_fee_after_release_that_just_ended():
    from forecast.engine import forecast_daily
    df, rel = _fee_history(); r_last = rel[-1]
    origin = r_last.start + pd.Timedelta(days=3); h = df.loc[:origin]
    point, _ = forecast_daily(h, origin, pd.date_range(origin + pd.Timedelta(days=1), periods=30), _fee_ctx(h, origin))
    fd = r_last.start + pd.Timedelta(days=10)
    assert point.loc[fd, 5] > 0.5 * df.loc[fd, 5]


def test_monthly_fee_rows_are_release_only():
    from forecast.monthly import monthly_forecast
    df, rel = _fee_history(); origin = pd.Timestamp("2026-10-23")
    ctx = _fee_ctx(df, origin)
    s = ctx.schedule[0]
    month = (s.start + pd.Timedelta(days=10)).replace(day=1)
    m = monthly_forecast(df, origin, ctx)
    row = m[(m.kpi_id == 5) & (m.month_start == month)].iloc[0]
    assert row.organic == 0 and row.fitted_growth == 1.0 and row.release > 0
