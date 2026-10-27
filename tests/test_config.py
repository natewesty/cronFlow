import pandas as pd

from forecast import config
from forecast.config import EngineParams
from forecast.types import Context, ScheduledRelease


def test_all_kpis_cover_1_to_31_exactly_once():
    assert config.ALL_KPIS == list(range(1, 32))
    modeled = set(config.MODELED_KPIS)
    derived = set(config.DERIVED_KPIS)
    assert modeled.isdisjoint(derived)
    assert modeled | derived == set(range(1, 32))


def test_params_defaults_and_overrides():
    p = config.EngineParams()
    assert p.lambda_shrink == 0.7 and p.w1 == 0.7 and p.w2 == 0.3
    assert p.trend_window == 91 and p.trend_clip == (0.5, 2.0)
    assert p.horizon == 120 and p.otb_days == 60
    assert p.release_min_value == 50000 and p.release_median_mult == 5
    assert p.release_lookback_n == 4 and p.attrition_spike_days == 21
    assert p.event_cap_pct == 0.95
    assert p.backtest_days == 365 and p.backtest_step == 7
    assert p.horizon_buckets == [(1, 7), (8, 28), (29, 91), (92, 120)]
    q = config.EngineParams.from_overrides({"lambda_shrink": 0.5, "unknown_key": 1})
    assert q.lambda_shrink == 0.5
    assert q.to_dict()["w1"] == 0.7


def test_growth_family_covers_every_kpi():
    assert set(config.GROWTH_FAMILY) == set(range(1, 34))
    assert set(config.GROWTH_FAMILY.values()) <= {
        "tasting_room", "club", "ecomm_phone_ship", "events", "membership", "wholesale"}


def test_kpi_families_v2():
    from forecast.config import FEE_KPIS, SEASONAL_KPIS, MODELED_KPIS, ENGINE_VERSION, season_of
    assert FEE_KPIS == [5] and 5 not in SEASONAL_KPIS and 5 in MODELED_KPIS
    assert ENGINE_VERSION == "3.0.0"
    assert season_of(pd.Timestamp("2026-02-25")) == "spring"
    assert season_of(pd.Timestamp("2026-09-02")) == "fall"
    assert season_of(pd.Timestamp("2026-11-18")) == "winter"


def test_engine_params_v2_defaults():
    p = EngineParams()
    assert (p.period_weeks_ahead, p.period_months_ahead, p.bottle_curve_months, p.tier_min_members, p.release_match_years) == (4, 3, 36, 10, 2)
    assert EngineParams.from_overrides({"tier_min_members": 5}).tier_min_members == 5


def test_context_v2_fields_default_none():
    ctx = Context(params=EngineParams(), releases=[], schedule=[])
    assert ctx.club_plan is None and ctx.tier_shares is None and ctx.label_plan is None and ctx.vintage_plan is None
    assert ScheduledRelease(start=pd.Timestamp("2027-09-01"), days=2, label="x").planned_revenue is None


def test_selectable_kpis():
    assert config.SELECTABLE_KPIS == [1, 2, 4, 7, 8, 13, 15, 16, 18, 22, 28, 29, 30, 31]
    assert EngineParams.from_overrides({"champion_weights": [0, 1]}).champion_weights == [0.0, 1.0]
    assert isinstance(EngineParams().to_dict()["champion_weights"], list)
