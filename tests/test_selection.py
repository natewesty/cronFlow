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


def test_zero_actual_month_kept_when_models_disagree():
    a = [100.0] * 6 + [0.0] * 2
    sw = [110.0] * 6 + [0.0] * 2
    lv = [104.0] * 6 + [300.0] * 2
    r = select_champions(frame(8, a, sw), frame(8, a, lv), P)[8]
    assert r.model == "swm" and r.n == 8 and r.wmape_default == pytest.approx(0.1)
    cell = next(g for g in r.candidates if g["w"] == 0.0 and not g["cal"])
    assert cell["wmape"] == pytest.approx(1.04)


def _margin_case(level_err):
    p = EngineParams(calibration_max=0.0, champion_weights=[0.0, 1.0])
    sw = [110.0, 90.0] * 5 + [110.0]
    lv = [100.0 + level_err, 100.0 - level_err] * 5 + [100.0 + level_err]
    return select_champions(frame(8, A, sw), frame(8, A, lv), p)[8]


def test_challenger_exactly_margin_better_is_adopted():
    r = _margin_case(9.5)
    assert r.model == "level" and r.w_swm == 0.0


def test_equal_score_keeps_default():
    r = _margin_case(10.0)
    assert r.model == "swm" and r.w_swm == 1.0


def test_nan_rows_are_dropped():
    sw = [110.0] * 11
    lv = [100.0] * 10 + [float("nan")]
    r = select_champions(frame(8, A, sw), frame(8, A, lv), P)[8]
    assert r.n == 10


def test_champion_weights_always_include_default():
    assert 1.0 in EngineParams.from_overrides({"champion_weights": [0.0, 0.5]}).champion_weights


def test_club_kpi_never_adopts_calibration():
    # only the calibrated cells beat the default (same data as test_calibration_adopted_and_bounded)
    lv = [300.0] * 11
    r = select_champions(frame(4, A, [120.0] * 11), frame(4, A, lv), P)[4]
    assert r.w_swm == 1.0 and r.model == "swm" and r.calibration == 1.0
    assert r.wmape_selection == pytest.approx(0.2) and r.wmape_default == pytest.approx(0.2)
    cal = next(g for g in r.candidates if g["w"] == 1.0 and g["cal"])
    assert cal["wmape"] < 0.2 - P.champion_margin       # still scored, just never adopted
    assert len(r.candidates) == 10


def test_club_kpi_can_still_take_an_uncalibrated_challenger():
    sw = [110.0, 90.0] * 5 + [110.0]
    lv = [105.0, 95.0] * 5 + [105.0]
    r = select_champions(frame(4, A, sw), frame(4, A, lv), P)[4]
    assert r.model == "level" and r.calibration == 1.0


def _guard_frames(f16_swm):
    """KPI 20 = (2 + 5 + 10) / 16 (config.RATIO_IDENTITIES). KPI 2's level forecast beats
    its swm forecast on 2 alone; 16 has no challenger; 5 and 10 are not selectable."""
    from forecast.config import RATIO_IDENTITIES
    assert RATIO_IDENTITIES[20] == ([2, 5, 10], [16], 1.0)
    a2, a5, a10, a16 = [100.0] * 11, [5.0] * 11, [0.0] * 11, [10.0] * 11
    sw2 = [110.0, 90.0] * 5 + [110.0]                    # wmape 0.10
    lv2 = [103.0, 97.0] * 5 + [103.0]                    # wmape 0.03
    f5 = [5.5, 4.5] * 5 + [5.5]                          # with sw2 and f16 the swm ratio is exact
    swm = pd.concat([frame(2, a2, sw2), frame(5, a5, f5), frame(10, a10, a10), frame(16, a16, f16_swm)])
    level = pd.concat([frame(2, a2, lv2), frame(5, a5, f5), frame(10, a10, a10), frame(16, a16, f16_swm)])
    return swm, level


def test_headline_guard_reverts_component_that_hurts_ratio():
    from forecast.selection import _GuardFrames
    swm, level = _guard_frames([11.0, 9.0] * 5 + [11.0])  # (110 + 5.5) / 11 = (90 + 4.5) / 9 = 10.5: exact
    from forecast.types import Champion
    r = select_champions(swm, level, P)
    # without the guard KPI 2 would take level (0.03 vs 0.10); KPI 20 would go from 0 to ~0.067
    lvl = {2: Champion(w_swm=0.0, model="level")}
    chosen, dflt = _GuardFrames(swm, level).wmapes(20, lvl, {})
    assert dflt == pytest.approx(0.0, abs=1e-12) and chosen > P.champion_margin
    c2 = r[2]
    assert c2.model == "swm" and c2.w_swm == 1.0 and c2.calibration == 1.0 and c2.reverted_for == 20
    assert c2.wmape_default == pytest.approx(0.10) and c2.wmape_selection == pytest.approx(0.10)
    assert c2.n == 11 and len(c2.candidates) == 10
    assert min(g["wmape"] for g in c2.candidates) == pytest.approx(0.03)
    assert r[16].model == "swm" and r[16].reverted_for is None


def test_headline_guard_keeps_champion_that_helps_ratio():
    swm, level = _guard_frames([10.0] * 11)               # 16 exact: the better KPI 2 helps 20 too
    r = select_champions(swm, level, P)
    assert r[2].model == "level" and r[2].w_swm == 0.0 and r[2].reverted_for is None
    assert r[2].wmape_selection == pytest.approx(0.03)


def test_headline_guard_picks_the_most_helpful_reversion():
    from forecast.selection import headline_guard
    from forecast.types import Champion
    swm, level = _guard_frames([11.0, 9.0] * 5 + [11.0])
    # both parts of 20 off their defaults: 2 on level (hurts 20), 16 on swm x 1.001 (barely matters)
    champs = {2: Champion(w_swm=0.0, model="level"), 16: Champion(w_swm=1.0, calibration=1.001)}
    out = headline_guard(champs, swm, level, P)
    assert out[2].reverted_for == 20 and out[2].is_default
    assert out[16].calibration == 1.001 and out[16].reverted_for is None


def test_champion_weights_are_clipped_to_unit_interval():
    p = EngineParams.from_overrides({"champion_weights": [-0.5, 0.5, 1.5, 1.0]})
    assert p.champion_weights == [0.0, 0.5, 1.0]
