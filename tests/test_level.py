import numpy as np, pandas as pd
from forecast.config import EngineParams
from forecast.models.level import level_forecast

P = EngineParams()
ORIGIN = pd.Timestamp("2026-10-23")
DATES = pd.date_range("2026-10-24", periods=60, freq="D")

def series(values, start="2023-07-01", end="2026-10-23"):
    idx = pd.date_range(start, end, freq="D")
    v = values(idx) if callable(values) else np.full(len(idx), float(values))
    return pd.Series(v, index=idx, dtype=float)

def test_constant_history_returns_level():
    fc = level_forecast(series(10.0), ORIGIN, DATES, P)
    assert np.allclose(fc.to_numpy(), 10.0)

def test_weekday_profile_is_reproduced():
    y = series(lambda idx: np.where(idx.dayofweek >= 5, 20.0, 10.0))
    fc = level_forecast(y, ORIGIN, DATES, P)
    assert np.allclose(fc[fc.index.dayofweek >= 5], 20.0) and np.allclose(fc[fc.index.dayofweek < 5], 10.0)

def test_seasonal_index_is_clipped_and_shrunk():
    # Twice the level over every numerator window of the last 32 target dates, none of the denominator window
    lo, hi = DATES[28] - pd.Timedelta(days=364 + 27), DATES[-1] - pd.Timedelta(days=364)
    y = series(lambda idx: np.where((idx >= lo) & (idx <= hi), 20.0, 10.0))
    fc = level_forecast(y, ORIGIN, DATES, P)
    assert np.allclose(fc.to_numpy()[28:], 17.0) and (fc.to_numpy()[:28] < 17.0).all() and fc.iloc[0] == 10.0

def test_all_zero_history_forecasts_zero():
    fc = level_forecast(series(0.0), ORIGIN, DATES, P)
    assert np.all(np.isfinite(fc)) and np.allclose(fc, 0.0)

def test_short_history_has_unit_index():
    fc = level_forecast(series(10.0, start="2026-08-01"), ORIGIN, DATES, P)
    assert np.allclose(fc.to_numpy(), 10.0)

def test_no_history_is_zero():
    fc = level_forecast(pd.Series(dtype=float), ORIGIN, DATES, P)
    assert np.allclose(fc, 0.0)
