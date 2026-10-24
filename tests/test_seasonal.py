import numpy as np
import pandas as pd

from forecast.config import EngineParams
from forecast.models.seasonal import (apply_floor, same_weekday_base,
                                      swm_forecast, trend_factor)
from tests.synth import WEEKLY

P = EngineParams()


def _exp_series(start="2023-07-01", end="2026-10-23", growth=0.05, level=100.0):
    idx = pd.date_range(start, end, freq="D")
    t = np.arange(len(idx)) / 365.25
    return pd.Series(level * WEEKLY[idx.dayofweek] * (1 + growth) ** t, index=idx)


def test_trend_factor_constant_series_is_one():
    y = pd.Series(5.0, index=pd.date_range("2024-01-01", "2026-10-23"))
    assert trend_factor(y, pd.Timestamp("2026-10-23"), 364, P) == 1.0


def test_trend_factor_is_clipped_then_shrunk():
    idx = pd.date_range("2024-01-01", "2026-10-23")
    y = pd.Series(np.where(idx >= pd.Timestamp("2026-01-01"), 10.0, 1.0), index=idx)
    # cur/prev = 10 -> clipped to 2.0 -> 1 + 0.7 * (2 - 1) = 1.7
    assert abs(trend_factor(y, pd.Timestamp("2026-10-23"), 364, P) - 1.7) < 1e-9


def test_trend_factor_zero_history_is_one():
    idx = pd.date_range("2024-01-01", "2026-10-23")
    y = pd.Series(np.where(idx >= pd.Timestamp("2026-06-01"), 3.0, 0.0), index=idx)
    assert trend_factor(y, pd.Timestamp("2026-10-23"), 364, P) == 1.0


def test_trend_factor_partial_prior_window_is_one():
    idx = pd.date_range("2025-09-01", "2026-10-23")
    y = pd.Series(np.where(idx >= pd.Timestamp("2026-07-01"), 2.0, 1.0), index=idx)
    # lagged window starts 2025-07-26, before history begins -> no spurious uplift
    assert trend_factor(y, pd.Timestamp("2026-10-23"), 364, P) == 1.0


def test_same_weekday_base_averages_three_lags_and_handles_holidays():
    idx = pd.date_range("2024-12-01", "2026-01-31")
    y = pd.Series(1.0, index=idx)
    y[pd.Timestamp("2025-01-01")] = 50.0          # Wednesday holiday spike
    y[pd.Timestamp("2025-01-08")] = 4.0
    y[pd.Timestamp("2025-01-15")] = 7.0
    dates = pd.DatetimeIndex([pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-14")])
    b = same_weekday_base(y, dates, 364)
    assert b[pd.Timestamp("2026-01-01")] == 50.0   # calendar-date alignment on Jan 1
    # 2026-01-14 - 364 = 2025-01-15 (Wed); ±7 -> Jan 8 (4.0), Jan 15 (7.0), Jan 22 (1.0)
    assert abs(b[pd.Timestamp("2026-01-14")] - 4.0) < 1e-9


def test_swm_tracks_trend_and_weekday_pattern():
    y = _exp_series()
    origin = pd.Timestamp("2026-10-23")
    dates = pd.date_range("2026-10-24", periods=28)
    f = swm_forecast(y, origin, dates, P)
    truth = _exp_series(end="2026-11-30").reindex(dates)
    rel = (f / truth - 1).abs()
    assert rel.max() < 0.03
    sat = f[dates.dayofweek == 5].mean()
    tue = f[dates.dayofweek == 1].mean()
    assert abs(sat / tue - WEEKLY[5] / WEEKLY[1]) < 0.05


def test_short_history_uses_single_base():
    y = _exp_series(start="2025-09-01")       # ~14 months: only the 364 lag exists
    f = swm_forecast(y, pd.Timestamp("2026-10-23"), pd.date_range("2026-10-24", periods=10), P)
    assert f.notna().all() and (f > 0).all()


def test_all_zero_series_forecasts_zero():
    y = pd.Series(0.0, index=pd.date_range("2023-07-01", "2026-10-23"))
    f = swm_forecast(y, pd.Timestamp("2026-10-23"), pd.date_range("2026-10-24", periods=120), P)
    assert f.notna().all() and (f == 0).all()


def test_no_lagged_data_falls_back_to_recent_mean():
    y = pd.Series(3.0, index=pd.date_range("2026-10-01", "2026-10-23"))
    f = swm_forecast(y, pd.Timestamp("2026-10-23"), pd.date_range("2026-10-24", periods=5), P)
    assert (f == 3.0).all()


def test_apply_floor():
    fc = pd.Series([-5.0, 2.0], index=pd.date_range("2026-10-24", periods=2))
    y = pd.Series(np.linspace(-100, 100, 201))
    assert apply_floor(fc, 1, y).tolist() == [0.0, 2.0]
    floored = apply_floor(fc, 4, y)                 # 5th percentile of y is -90
    assert floored.tolist() == [-5.0, 2.0]
    fc2 = pd.Series([-500.0], index=pd.date_range("2026-10-24", periods=1))
    assert apply_floor(fc2, 13, y).iloc[0] == -90.0
