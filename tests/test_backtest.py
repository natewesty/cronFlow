import numpy as np
import pandas as pd

from forecast.backtest import (ACCURACY_COLUMNS, ACCURACY_PERIOD_COLUMNS, PERIOD_COLUMNS,
                               period_frame, run_period_backtest, summarize_periods, apply_bands, backtest_frame, bucket_for,
                               run_backtest, run_backtest_full, summarize, uniform_champions)
from forecast.types import Champion
from forecast.config import SELECTABLE_KPIS, EngineParams
from forecast.models.club import detect_releases
from tests.synth import make_history

P = EngineParams(backtest_days=120, backtest_step=14)
ORIGIN = pd.Timestamp("2026-10-23")


def test_bucket_for():
    assert bucket_for(1, P) == "1-7" and bucket_for(7, P) == "1-7"
    assert bucket_for(8, P) == "8-28" and bucket_for(91, P) == "29-91"
    assert bucket_for(120, P) == "92-120" and bucket_for(500, P) == "92-120"


def test_backtest_drops_horizons_without_actuals():
    df = make_history()
    rel = detect_releases(df[4], P)
    bt = backtest_frame(df, rel, P, ORIGIN)
    assert bt["date"].max() == ORIGIN
    assert bt["actual"].notna().all()
    origins = sorted(bt["origin"].unique())
    assert len(origins) == 120 // 14
    last = bt[bt["origin"] == origins[-1]]
    assert last["horizon"].max() == 14


def test_run_backtest_accuracy_is_sane():
    df = make_history()
    rel = detect_releases(df[4], P)
    acc = run_backtest(df, rel, P, ORIGIN)
    assert list(acc.columns) == ACCURACY_COLUMNS
    one = acc[(acc.kpi_id == 1) & (acc.horizon_bucket == "1-7")].iloc[0]
    assert one.wmape < 0.2 and one.ratio_p10 < 1 < one.ratio_p90
    assert one.n > 0
    row = acc[(acc.kpi_id == 1) & (acc.horizon_bucket == "8-28")].iloc[0]
    assert row.weekly_wmape is not None and row.weekly_wmape <= one.wmape + 0.1
    assert set(acc.kpi_id) == set(range(1, 32))


def test_run_backtest_full_returns_summary_and_frame():
    df = make_history()
    rel = detect_releases(df[4], P)
    acc, bt = run_backtest_full(df, rel, P, ORIGIN)
    pd.testing.assert_frame_equal(acc, run_backtest(df, rel, P, ORIGIN))
    pd.testing.assert_frame_equal(bt, backtest_frame(df, rel, P, ORIGIN))


def test_wmape_null_when_no_actuals():
    df = make_history()
    df[9] = 0.0
    df = df.copy()
    rel = detect_releases(df[4], P)
    acc = run_backtest(df, rel, P, ORIGIN)
    nine = acc[acc.kpi_id == 9]
    assert nine["wmape"].isna().all() and nine["ratio_p10"].isna().all()


def test_apply_bands_multiplies_by_bucket_ratio():
    dates = pd.date_range("2026-10-24", periods=10)
    point = pd.DataFrame({1: 100.0, 2: 50.0}, index=dates)
    acc = pd.DataFrame([
        {"kpi_id": 1, "horizon_bucket": "1-7", "ratio_p10": 0.8, "ratio_p90": 1.2},
        {"kpi_id": 1, "horizon_bucket": "8-28", "ratio_p10": 0.5, "ratio_p90": 1.5},
    ])
    p10, p90 = apply_bands(point, acc, P)
    assert p10.loc[dates[0], 1] == 80 and p90.loc[dates[0], 1] == 120
    assert p10.loc[dates[9], 1] == 50 and p90.loc[dates[9], 1] == 150
    assert np.isnan(p10.loc[dates[0], 2])


def test_backtest_origin_on_release_day_one_forecasts_day_two():
    df = make_history(rpm=300.0)
    rel = detect_releases(df[4], P)
    o = rel[-1].start
    p = EngineParams(backtest_days=7, backtest_step=3)
    bt = backtest_frame(df, rel, p, o + pd.Timedelta(days=3))
    row = bt[(bt.origin == o) & (bt.kpi_id == 4) & (bt.horizon == 1)]
    assert len(row) == 1 and row.iloc[0].forecast > 0


def test_apply_bands_negative_point_keeps_p10_below_p90():
    dates = pd.date_range("2026-10-24", periods=3)
    point = pd.DataFrame({26: -10.0}, index=dates)
    acc = pd.DataFrame([{"kpi_id": 26, "horizon_bucket": "1-7", "ratio_p10": 0.8, "ratio_p90": 1.2}])
    p10, p90 = apply_bands(point, acc, P)
    assert (p10[26] <= p90[26]).all()


def test_apply_bands_zero_forecast_gets_zero_band_even_without_ratio():
    # A KPI forecast at 0 has no finite actual/forecast ratios (production KPI 5);
    # its band is 0, not NaN, so sums such as 6 and 14 keep their bands.
    dates = pd.date_range("2026-10-24", periods=3)
    point = pd.DataFrame({1: 100.0, 5: 0.0}, index=dates)
    acc = pd.DataFrame([
        {"kpi_id": 1, "horizon_bucket": "1-7", "ratio_p10": 0.8, "ratio_p90": 1.2},
        {"kpi_id": 5, "horizon_bucket": "1-7", "ratio_p10": np.nan, "ratio_p90": np.nan},
    ])
    p10, p90 = apply_bands(point, acc, P)
    assert (p10[5] == 0).all() and (p90[5] == 0).all()
    assert (p10[1] == 80).all()


def test_backtest_schedule_carries_season(monkeypatch):
    import forecast.backtest as bm
    from forecast.config import SEASONS
    seen = []
    real = bm.forecast_daily

    def spy(hist, o, dates, ctx):
        seen.extend(ctx.schedule)
        return real(hist, o, dates, ctx)
    monkeypatch.setattr(bm, "forecast_daily", spy)
    df = make_history(); rel = detect_releases(df[4], P)
    backtest_frame(df, rel, P, ORIGIN)
    assert seen and all(x.season in SEASONS for x in seen)


def test_period_frame_only_full_periods():
    df = make_history(); rel = detect_releases(df[4], P)
    bt = backtest_frame(df, rel, P, ORIGIN)
    pf = period_frame(bt, P)
    months = pf[(pf.period == "month") & (pf.kpi_id == 1)]
    for r in months.itertuples():
        start = pd.Timestamp(r.period_start); end = start + pd.offsets.MonthEnd(0)
        o = pd.Timestamp(r.origin_date)
        assert start > o and end <= o + pd.Timedelta(days=P.horizon)
    weeks = pf[(pf.period == "week") & (pf.kpi_id == 1)]
    assert (pd.to_datetime(weeks.period_start).dt.dayofweek == 0).all()
    assert months.ahead.max() <= P.period_months_ahead and weeks.ahead.max() <= P.period_weeks_ahead


def test_period_ahead_uses_last_origin_before_period():
    df = make_history(); rel = detect_releases(df[4], P)
    pf = period_frame(backtest_frame(df, rel, P, ORIGIN), P)
    m1 = pf[(pf.period == "month") & (pf.ahead == 1) & (pf.kpi_id == 1)]
    assert not m1.duplicated(["period_start"]).any()
    for r in m1.itertuples():
        start = pd.Timestamp(r.period_start)
        origins = sorted(pf[(pf.kpi_id == 1)].origin_date.unique())
        assert pd.Timestamp(r.origin_date) == max(o for o in map(pd.Timestamp, origins) if o < start)


def test_summarize_periods_hit_rate_near_eighty_percent():
    df = make_history(); rel = detect_releases(df[4], P)
    periods, acc = run_period_backtest(backtest_frame(df, rel, EngineParams(backtest_days=365, backtest_step=7), ORIGIN), P)
    row = acc[(acc.kpi_id == 1) & (acc.period == "month") & (acc.ahead == 1)].iloc[0]
    assert 0.6 <= row.hit_rate <= 1.0 and row.n >= 8 and row.wmape < 0.2
    assert list(periods.columns) == PERIOD_COLUMNS and list(acc.columns) == ACCURACY_PERIOD_COLUMNS


def test_ratio_kpi_periods_use_aggregate_period():
    df = make_history(); rel = detect_releases(df[4], P)
    pf = period_frame(backtest_frame(df, rel, P, ORIGIN), P)
    r17 = pf[(pf.kpi_id == 17) & (pf.period == "month")].iloc[0]
    assert 2 < r17.actual < 6   # a ratio, not a sum of daily ratios


def _snap(o):
    return pd.DataFrame({"snapshot_date": [o + pd.Timedelta(days=1)] * 3,
                         "reservation_date": pd.date_range(o + pd.Timedelta(days=1), periods=3),
                         "attribution": "Walk-in", "reservations": 10_000, "guests": 40_000, "fees": 9e6})


def test_backtest_with_champions_keeps_identities():
    H = make_history(); releases = detect_releases(H[4], P)
    ch = {1: Champion(w_swm=0.0), 4: Champion(w_swm=0.5, calibration=1.05)}
    bt = backtest_frame(H, releases, P, ORIGIN, champions=ch)
    wide = bt.pivot_table(index=["origin", "date"], columns="kpi_id", values="forecast")
    assert np.allclose(wide[3], wide[1] + wide[2]) and np.allclose(wide[6], wide[4] + wide[5])


def test_snapshot_otb_applies_to_matching_origin_only():
    H = make_history(); releases = detect_releases(H[4], P)
    o = ORIGIN - pd.Timedelta(days=14)
    snap = _snap(o)
    plain = backtest_frame(H, releases, P, ORIGIN)
    with_snap = backtest_frame(H, releases, P, ORIGIN, snapshots=snap)
    k = 15
    a = plain[plain.kpi_id == k].set_index(["origin", "date"]).forecast
    b = with_snap[with_snap.kpi_id == k].set_index(["origin", "date"]).forecast
    diff = (a != b).groupby(level="origin").any()
    assert diff[o] and not diff.drop(o).any()
    assert with_snap.calendar[with_snap.origin == o].all() and not with_snap.calendar[with_snap.origin != o].any()
    assert not plain.calendar.any()


def test_period_frame_carries_calendar_flag():
    H = make_history(); releases = detect_releases(H[4], P)
    o = ORIGIN - pd.Timedelta(days=14)
    pf = period_frame(backtest_frame(H, releases, P, ORIGIN, snapshots=_snap(o)), P)
    assert list(pf.columns) == ["kpi_id", "period", "ahead", "period_start", "origin_date", "actual", "forecast", "calendar"]
    assert pf.calendar[pf.origin_date == o].all() and not pf.calendar[pf.origin_date != o].any()
    assert (pf.origin_date == o).any()


def test_summarize_periods_excludes_both_zero_months_and_has_mae():
    pf = pd.DataFrame({"kpi_id": 5, "period": "month", "ahead": 1,
                       "period_start": pd.date_range("2026-01-01", periods=6, freq="MS"), "origin_date": pd.Timestamp("2025-12-31"),
                       "actual": [0, 0, 100, 0, 0, 50], "forecast": [0, 0, 120, 0, 0, 50], "calendar": False})
    periods, acc = summarize_periods(pf)
    row = acc.iloc[0]
    assert row.n == 2 and np.isclose(row.wmape, 20 / 150) and np.isclose(row.mae, 10.0)
    assert len(periods) == 6 and list(acc.columns) == ACCURACY_PERIOD_COLUMNS


def test_uniform_champions_covers_selectable_only():
    u = uniform_champions(0.0)
    assert set(u) == set(SELECTABLE_KPIS) and all(c.w_swm == 0.0 for c in u.values())
