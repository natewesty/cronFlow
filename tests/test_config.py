from forecast import config


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
