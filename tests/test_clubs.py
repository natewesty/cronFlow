import numpy as np
import pandas as pd
import pytest

from forecast.clubs import (HISTORY_COLUMNS, active_on, attribute_orders, release_history,
                            take_rate_default, tier_table)
from forecast.config import EngineParams
from tests.synth_bottles import make_club_orders, make_members_daily, make_memberships

P = EngineParams()


def test_attribution_tie_break_by_box_size():
    m = make_memberships(); rel = pd.Timestamp("2026-09-02"); o = make_club_orders(m, [rel])
    tiers = tier_table(m, rel, 10)
    a = attribute_orders(o, m, rel, tiers)
    dual = m[m.customer_id.duplicated(keep=False) & (m.status == "Active")].customer_id.unique()
    rows = a[a.customer_id.isin(dual)]
    assert set(rows.groupby("order_id").tier_id.nunique()) == {1}
    assert (rows.tier_id == tiers[tiers.tier_name == "Premier 6"].tier_id.iloc[0]).all()
    assert a.order_id.nunique() == o.order_id.nunique()


def test_unmatched_orders_go_to_other():
    m = make_memberships(); rel = pd.Timestamp("2026-09-02"); o = make_club_orders(m, [rel])
    stray = o.iloc[:1].copy(); stray["order_id"] = "stray"; stray["customer_id"] = "nobody"
    a = attribute_orders(pd.concat([o, stray]), m, rel, tier_table(m, rel, 10))
    assert a[a.order_id == "stray"].tier_id.iloc[0] == "other"


def test_release_history_take_rate_and_allocation():
    m = make_memberships(); rel = pd.Timestamp("2026-09-02"); o = make_club_orders(m, [rel])
    tiers = tier_table(m, rel, 10); md = make_members_daily(m, "2026-01-01", "2026-10-25")
    h = release_history({rel: o}, m, md, tiers, P)
    assert list(h.columns) == HISTORY_COLUMNS
    est = h[(h.tier_name == "Estate")]
    assert abs(est.take_rate.iloc[0] - 0.78) < 0.03 and est.season.iloc[0] == "fall"
    # fixture allocation for Estate is 2/1/1 bottles across three labels
    assert sorted(est.qty_per_box) == [1, 1, 2] and len(est) == 3
    p4 = h[h.tier_name == "Premier 4"]
    assert set(p4.qty_per_box) == {2} and len(p4) == 2
    assert "Tiny" not in set(h.tier_name)


def test_take_rate_default_trend_and_clip():
    h = pd.DataFrame({"release_start": pd.to_datetime(["2023-09-06", "2024-09-04", "2025-09-03", "2026-09-02"]), "season": "fall", "tier_id": "t", "take_rate": [0.70, 0.68, 0.66, 0.64]})
    assert take_rate_default(h, "t", "fall") == pytest.approx(0.62, abs=0.005)
    assert take_rate_default(h.assign(take_rate=[0.2, 0.2, 0.2, 0.2]), "t", "fall") == 0.3
    assert take_rate_default(h, "zzz", "fall") == 0.6


def test_active_on_null_cancel_non_active_is_inactive():
    m = pd.DataFrame({"membership_id": [1, 2], "customer_id": [1, 2], "club_id": "c", "club_name": "C",
                      "status": ["Active", "Paused"], "signup_at": pd.to_datetime(["2020-01-01"] * 2),
                      "cancel_at": pd.NaT, "delivery_method": "Ship"})
    assert list(active_on(m, pd.Timestamp("2026-01-01")).membership_id) == [1]
