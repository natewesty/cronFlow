import numpy as np
import pandas as pd
import pytest

from forecast.config import EngineParams
from forecast.selection import grid_scores, loo_calibration, month_ahead_points, select_champions

P = EngineParams()
MONTHS = pd.date_range("2026-01-01", periods=11, freq="MS")


def frame(kpi, actual, forecast, months=None):
    months = MONTHS[:len(actual)] if months is None else months
    return pd.DataFrame({"kpi_id": kpi, "period": "month", "ahead": 1, "period_start": months,
                         "origin_date": months - pd.Timedelta(days=1), "actual": actual,
                         "forecast": forecast, "calendar": False})


A = [100.0] * 11


def test_month_ahead_points_filters():
    pf = pd.concat([frame(1, [0.0, 5.0], [0.0, 4.0]),
                    frame(1, [9.0], [9.0]).assign(ahead=2),
                    frame(1, [9.0], [9.0]).assign(period="week")])
    pts = month_ahead_points(pf)
    assert len(pts) == 1 and pts.iloc[0]["actual"] == 5.0


def test_default_kept_when_challenger_inside_margin():
    # swm off by 10 each month (0.10), level off by 9.7 (0.097): better by 0.003 < 0.005
    sw = [110.0, 90.0] * 5 + [110.0]
    lv = [109.7, 90.3] * 5 + [109.7]
    # alternating errors cancel in calibration cells only through LOO; keep cal cells unattractive
    r = select_champions(frame(8, A, sw), frame(8, A, lv), P)[8]
    assert r.w_swm == 1.0 and r.model == "swm" and r.calibration == 1.0
    assert r.wmape_default == pytest.approx(0.10) and r.wmape_selection == pytest.approx(0.10)


def test_level_adopted_when_better_by_margin():
    sw = [110.0, 90.0] * 5 + [110.0]      # wmape 0.10
    lv = [105.0, 95.0] * 5 + [105.0]      # wmape 0.05
    r = select_champions(frame(8, A, sw), frame(8, A, lv), P)[8]
    assert r.w_swm == 0.0 and r.model == "level" and r.calibration == 1.0
    assert r.wmape_selection == pytest.approx(0.05) and r.wmape_default == pytest.approx(0.10)
    assert r.n == 11 and len(r.candidates) == 10


def test_blend_cell_can_win():
    sw = [130.0, 120.0] * 5 + [130.0]     # high
    lv = [70.0, 80.0] * 5 + [70.0]        # low; midpoint is exactly 100/100
    r = select_champions(frame(8, A, sw), frame(8, A, lv), P)[8]
    assert r.w_swm == 0.5 and r.model == "blend"
    assert r.wmape_selection == pytest.approx(0.0, abs=1e-12) and r.calibration == 1.0


def test_too_few_months_keeps_default():
    a = [100.0] * 5
    r = select_champions(frame(8, a, [150.0] * 5), frame(8, a, [100.0] * 5), P)[8]
    assert r.w_swm == 1.0 and r.model == "swm" and r.n == 5 and r.calibration == 1.0
    assert r.wmape_default == pytest.approx(0.5)


def test_zero_actuals_calibration_off():
    a = [0.0] * 8
    r = select_champions(frame(8, a, [3.0] * 8), frame(8, a, [1.0] * 8), P)[8]
    assert r.w_swm == 1.0 and r.calibration == 1.0 and r.n == 8
    assert r.candidates == [] and r.wmape_default is None


def test_calibration_adopted_and_bounded():
    lv = [300.0] * 11
    r = select_champions(frame(8, A, [120.0] * 11), frame(8, A, lv), P)[8]
    assert r.w_swm == 1.0 and r.model == "swm"
    assert r.calibration == pytest.approx(1 / (1 + 0.5 * 0.2))
    assert r.wmape_default == pytest.approx(0.2) and r.wmape_selection == pytest.approx(0.1 / 1.1)
    r2 = select_champions(frame(8, A, [150.0] * 11), frame(8, A, lv), P)[8]
    assert r2.w_swm == 1.0 and r2.calibration == 1.0 and r2.wmape_selection == pytest.approx(0.5)


def test_loo_calibration_formula():
    m = loo_calibration(np.array([100.0] * 3), np.array([120.0, 120.0, 90.0]), 0.5)
    assert m[0] == pytest.approx(1 / (1 + 0.5 * ((120 + 90) / 200 - 1)))
    assert m[0] == pytest.approx(1 / 1.025)
    assert loo_calibration(np.array([0.0, 0.0]), np.array([1.0, 2.0]), 0.5).tolist() == [1.0, 1.0]


def test_grid_has_ten_cells_and_non_selectable_ignored():
    g = grid_scores(np.array(A), np.array([110.0] * 11), np.array([90.0] * 11), P)
    assert len(g) == 10 and {(c["w"], c["cal"]) for c in g} == {(w, c) for w in P.champion_weights for c in (False, True)}
    assert grid_scores(np.zeros(3), np.ones(3), np.ones(3), P) == []
    both = pd.concat([frame(8, A, A), frame(24, A, A)])
    assert set(select_champions(both, both, P)) == {8}
