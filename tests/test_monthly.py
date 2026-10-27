import numpy as np
import pytest
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


def test_second_year_organic_compounds_growth():
    df = make_history(annual_growth=0.05)
    m = monthly_forecast(df, ORIGIN, _ctx(df))
    r1 = m[(m.kpi_id == 1) & (m.month_start == "2026-12-01")].iloc[0]
    r2 = m[(m.kpi_id == 1) & (m.month_start == "2027-12-01")].iloc[0]
    assert abs(r2.organic / r1.organic - r2.fitted_growth) < 0.01


def test_aov_monthly_averages_only_order_days():
    df = make_history()
    df.loc["2025-11-01":"2025-11-10", 28] = 0
    df.loc["2025-11-01":"2025-11-10", 29] = 0
    m = monthly_forecast(df, ORIGIN, _ctx(df))
    row = m[(m.kpi_id == 29) & (m.month_start == "2025-11-01")].iloc[0]
    nov = df.loc["2025-11"]
    assert abs(row.p50 - nov[nov[28] > 0][29].mean()) < 1e-9
    assert not m.p50.isna().any()


def test_monthly_release_matches_daily_and_flags_plan():
    df = make_history(season_mult={"fall": 1.4, "summer": 0.6, "spring": 1.0, "winter": 1.0})
    origin = pd.Timestamp("2026-08-15"); h = df.loc[:origin]
    rel = detect_releases(h[4], P); sched = default_schedule(rel, origin)
    s = next(x for x in sched if x.start.month == 9)
    ctx = Context(params=P, releases=rel, schedule=sched, members_a0=float(h[23].iloc[-1]))
    m = monthly_forecast(h, origin, ctx)
    row = m[(m.kpi_id == 4) & (m.month_start == pd.Timestamp("2026-09-01"))].iloc[0]
    actual = df[4].reindex(s.dates).sum()
    assert abs(row.release - actual) / actual < 0.06 and row.release_planned == False
    plan = pd.DataFrame([["fall", pd.Timestamp("2026-09-01"), "t1", "A", 2024, 1, 1.0, 1.0, 400_000]],
                        columns=["season", "release_date", "tier_id", "label", "vintage", "qty", "price", "take_rate", "members_override"])
    m2 = monthly_forecast(h, origin, Context(params=P, releases=rel, schedule=sched, members_a0=ctx.members_a0, club_plan=plan))
    row2 = m2[(m2.kpi_id == 4) & (m2.month_start == pd.Timestamp("2026-09-01"))].iloc[0]
    assert row2.release == pytest.approx(400_000, rel=1e-6) and row2.release_planned == True


def _with_champs(df, champs):
    ctx = _ctx(df)
    ctx.champions = champs
    return ctx


def _row(m, k, month):
    return m[(m.kpi_id == k) & (m.month_start == month)].iloc[0]


def test_default_champion_monthly_unchanged():
    from forecast.config import SELECTABLE_KPIS
    from forecast.types import Champion
    df = make_history()
    base = monthly_forecast(df, ORIGIN, _with_champs(df, {}))
    dflt = monthly_forecast(df, ORIGIN, _with_champs(df, {k: Champion.default() for k in SELECTABLE_KPIS}))
    pd.testing.assert_frame_equal(base, dflt)


# A 12-month profile (Jan..Dec) whose every month is within the 0.4-2.5 clip of the Jul-Sep base.
PROFILE = [70.0, 75.0, 90.0, 105.0, 120.0, 135.0, 150.0, 155.0, 145.0, 125.0, 100.0, 80.0]


def test_level_month_reproduces_pure_seasonal_series():
    from forecast.monthly import organic_month
    from forecast.types import Champion
    months = pd.date_range("2023-10-01", "2026-09-01", freq="MS")      # three full years, no trend
    mh = pd.Series([PROFILE[d.month - 1] for d in months], index=months)
    p = EngineParams(lambda_shrink=1.0)                                 # unshrunk index: exact seasonal change
    champ = Champion(w_swm=0.0, model="level")
    # first year from the year-ago lag, second year (no year-ago month yet) from two years back
    for month in pd.date_range("2026-10-01", periods=24, freq="MS"):
        got = organic_month(1, mh, {}, month, p, 1.0, champ)
        truth = PROFILE[month.month - 1]
        assert abs(got - truth) / truth < 0.05, (month, got, truth)
    # with the default shrink the index is pulled toward 1 but keeps its direction
    jan = organic_month(1, mh, {}, pd.Timestamp("2027-01-01"), P, 1.0, champ)
    base = (150.0 + 155.0 + 145.0) / 3
    assert jan == pytest.approx(base * (1.0 + P.lambda_shrink * (70.0 / base - 1.0)))


def test_level_month_index_is_one_when_a_year_ago_month_is_missing():
    from forecast.monthly import _level_month
    months = pd.date_range("2025-10-01", "2026-09-01", freq="MS")      # one year: base year-ago months absent
    mh = pd.Series([PROFILE[d.month - 1] for d in months], index=months)
    assert _level_month(mh, pd.Timestamp("2027-01-01"), P) == pytest.approx((150.0 + 155.0 + 145.0) / 3)
    assert _level_month(mh, pd.Timestamp("2028-01-01"), P) == pytest.approx((150.0 + 155.0 + 145.0) / 3)


def test_level_month_second_year_uses_two_year_lag_with_same_lag_base():
    from forecast.monthly import _level_month
    months = pd.date_range("2023-10-01", "2026-09-01", freq="MS")
    # trend between years: the two-year lag must be scaled by the base two years back, not one
    mh = pd.Series([PROFILE[d.month - 1] * (1.0 + 0.1 * (d.year - 2023)) for d in months], index=months)
    p = EngineParams(lambda_shrink=1.0)
    base = float(mh.iloc[-3:].mean())
    got = _level_month(mh, pd.Timestamp("2028-01-01"), p)
    lag2 = mh[pd.Timestamp("2026-01-01")] / mh[pd.Timestamp("2024-07-01"):pd.Timestamp("2024-09-01")].mean()
    assert got == pytest.approx(base * lag2)


def test_level_champion_monthly_mirrors_daily_level():
    from forecast.types import Champion
    df = make_history()
    ctx = _with_champs(df, {1: Champion(w_swm=0.0, model="level")})
    dates = pd.date_range(ORIGIN + pd.Timedelta(days=1), periods=P.horizon)
    point, _ = forecast_daily(df, ORIGIN, dates, ctx)
    m = monthly_forecast(df, ORIGIN, ctx, daily_point=point)
    full = [mo for mo in pd.date_range("2026-11-01", periods=4, freq="MS")
            if mo + pd.offsets.MonthEnd(0) <= dates[-1]]
    assert len(full) == 3                                               # Nov, Dec, Jan
    for mo in full:
        daily = float(point.loc[mo:mo + pd.offsets.MonthEnd(0), 1].sum())
        monthly = _row(m, 1, mo).organic
        assert abs(monthly - daily) / daily < 0.25, (mo, monthly, daily)


def test_monthly_calibration_scales_organic():
    from forecast.types import Champion
    df = make_history()
    base = monthly_forecast(df, ORIGIN, _with_champs(df, {}))
    cal = monthly_forecast(df, ORIGIN, _with_champs(df, {1: Champion(w_swm=1.0, calibration=1.1)}))
    b, c = _row(base, 1, pd.Timestamp("2027-02-01")), _row(cal, 1, pd.Timestamp("2027-02-01"))
    assert c.organic == pytest.approx(1.1 * b.organic)
    assert c.organic_neutral == pytest.approx(1.1 * b.organic_neutral)
