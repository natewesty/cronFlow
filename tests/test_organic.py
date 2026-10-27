import numpy as np, pandas as pd
from forecast.config import EngineParams
from forecast.models.level import level_forecast
from forecast.models.organic import organic_forecast
from forecast.models.seasonal import swm_forecast
from forecast.types import Champion
from tests.synth import make_history

P = EngineParams(); H = make_history(); ORIGIN = pd.Timestamp("2026-10-23")
DATES = pd.date_range("2026-10-24", periods=60, freq="D")

def test_no_champion_is_swm():
    assert organic_forecast(1, H[1], ORIGIN, DATES, P, {}).equals(swm_forecast(H[1], ORIGIN, DATES, P))

def test_default_champion_is_swm():
    assert organic_forecast(1, H[1], ORIGIN, DATES, P, {1: Champion.default()}).equals(swm_forecast(H[1], ORIGIN, DATES, P))

def test_weight_zero_is_level():
    assert organic_forecast(1, H[1], ORIGIN, DATES, P, {1: Champion(w_swm=0.0)}).equals(level_forecast(H[1], ORIGIN, DATES, P))

def test_blend_and_calibration():
    out = organic_forecast(1, H[1], ORIGIN, DATES, P, {1: Champion(w_swm=0.5, calibration=1.1)})
    want = 1.1 * (0.5 * swm_forecast(H[1], ORIGIN, DATES, P) + 0.5 * level_forecast(H[1], ORIGIN, DATES, P))
    assert np.allclose(out, want)

def test_non_selectable_kpi_ignores_champion():
    assert organic_forecast(24, H[24], ORIGIN, DATES, P, {24: Champion(w_swm=0.0)}).equals(swm_forecast(H[24], ORIGIN, DATES, P))

def test_champion_name():
    assert (Champion.name_for(1.0), Champion.name_for(0.0), Champion.name_for(0.25)) == ("swm", "level", "blend")

def test_full_swm_weight_still_applies_calibration():
    out = organic_forecast(1, H[1], ORIGIN, DATES, P, {1: Champion(w_swm=1.0, calibration=1.1)})
    assert np.allclose(out, 1.1 * swm_forecast(H[1], ORIGIN, DATES, P))
