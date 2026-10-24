import numpy as np
import pandas as pd

from forecast.config import ALL_KPIS, EngineParams
from forecast.engine import forecast_daily
from forecast.models.club import default_schedule, detect_releases
from forecast.monthly import MONTHLY_COLUMNS, monthly_forecast
from forecast.types import Context
from tests.synth import make_history

P = EngineParams()
ORIGIN = pd.Timestamp("2026-10-23")


def _ctx(df):
    rel = detect_releases(df[4], P)
    return Context(params=P, releases=rel, schedule=default_schedule(rel, ORIGIN), events=[],
                   otb=None, members_a0=float(df.loc[ORIGIN, 23]))


def test_shape_and_actual_months():
    df = make_history()
    m = monthly_forecast(df, ORIGIN, _ctx(df))
    assert list(m.columns) == MONTHLY_COLUMNS
    months = sorted(m.month_start.unique())
    assert months[0] == pd.Timestamp("2025-10-01") and months[-1] == pd.Timestamp("2028-09-01")
    assert len(months) == 36 and set(m.kpi_id) == set(ALL_KPIS)
    assert m[m.month_start < "2026-10-01"].is_actual.all()
    assert not m[m.month_start >= "2026-10-01"].is_actual.any()
    nov25 = m[(m.kpi_id == 1) & (m.month_start == "2025-11-01")].iloc[0]
    assert abs(nov25.p50 - df.loc["2025-11", 1].sum()) < 1e-6
    assert not m.p50.isna().any()


def test_organic_month_tracks_last_year_times_growth():
    df = make_history(annual_growth=0.05)
    m = monthly_forecast(df, ORIGIN, _ctx(df))
    row = m[(m.kpi_id == 1) & (m.month_start == "2027-02-01")].iloc[0]
    ly = df.loc["2026-02", 1].sum()
    assert 1.0 < row.fitted_growth < 1.06
    assert abs(row.organic_neutral - ly) / ly < 0.12
    assert abs(row.organic - row.organic_neutral * row.fitted_growth) < 1e-6
    assert row.release == 0 and row.event == 0


def test_club_release_lands_in_scheduled_month():
    df = make_history(rpm=300.0)
    ctx = _ctx(df)
    m = monthly_forecast(df, ORIGIN, ctx)
    feb = m[(m.kpi_id == 4) & (m.month_start == "2027-02-01")].iloc[0]
    jan = m[(m.kpi_id == 4) & (m.month_start == "2027-01-01")].iloc[0]
    members = m[(m.kpi_id == 23) & (m.month_start == "2027-01-01")].iloc[0].p50
    assert abs(feb.release - 300.0 * members) / (300.0 * members) < 0.05
    assert jan.release == 0 and feb.organic < 0.05 * feb.release
    nov25 = m[(m.kpi_id == 4) & (m.month_start == "2025-11-01")].iloc[0]
    assert nov25.is_actual and nov25.release > 0.9 * nov25.p50


def test_current_month_uses_actual_mtd_plus_daily_forecast():
    df = make_history()
    ctx = _ctx(df)
    dates = pd.date_range("2026-10-24", periods=120)
    point, _ = forecast_daily(df, ORIGIN, dates, ctx)
    m = monthly_forecast(df, ORIGIN, ctx, daily_point=point)
    oct_row = m[(m.kpi_id == 1) & (m.month_start == "2026-10-01")].iloc[0]
    expected = df.loc["2026-10-01":ORIGIN, 1].sum() + point.loc["2026-10-24":"2026-10-31", 1].sum()
    assert abs(oct_row.p50 - expected) < 1e-6


def test_identities_hold_monthly():
    df = make_history()
    m = monthly_forecast(df, ORIGIN, _ctx(df))
    wide = m.pivot(index="month_start", columns="kpi_id", values="p50")
    assert np.allclose(wide[14], wide[[1, 2, 4, 5, 7, 8, 9, 10, 12, 13]].sum(axis=1))
    assert np.allclose(wide[26], wide[24] - wide[25])


def test_components_rebuild_p50_for_forecast_months():
    df = make_history()
    m = monthly_forecast(df, ORIGIN, _ctx(df))
    f = m[(~m.is_actual) & m.kpi_id.isin([1, 4, 24, 25, 29])]
    assert np.allclose(f.organic, f.organic_neutral * f.fitted_growth)
    g = f[f.kpi_id != 29]
    assert np.allclose(g.p50, g.organic + g.release + g.event)
    assert (m[m.kpi_id == 23].fitted_growth == 1.0).all()
    r25 = m[(m.kpi_id == 25) & (m.month_start == "2027-02-01")].iloc[0]
    assert r25.release > 0
