import pandas as pd

from forecast import io as fio
from tests.synth_bottles import (TIER_ACTIVE, make_bottle_history, make_club_orders,
                                 make_members_daily, make_memberships)


def test_bottle_history_shape_and_curve():
    h = make_bottle_history()
    assert set(h.columns) == set(fio.EMPTY["bottle_history"])
    hr = h[(h.label == "Home Ranch Pinot Noir") & (h.vintage == 2022)]
    assert hr.month_start.min() == pd.Timestamp("2024-06-01")
    first12 = hr[hr.month_start < "2025-06-01"].bottles.sum() / hr.bottles.sum()
    assert abs(first12 - 0.48) < 0.02


def test_bottle_history_vintages_and_truncation():
    h = make_bottle_history()
    assert sorted(h[h.label == "Hypersonic Pinot Noir"].vintage.unique()) == [2023, 2024]
    assert h[h.label == "Hypersonic Pinot Noir"].month_start.min() == pd.Timestamp("2025-07-01")
    assert list(h[h.label.str.startswith("Mikado")].vintage.unique()) == [2022]
    t = make_bottle_history(truncate=True)
    assert t.month_start.max() <= pd.Timestamp("2026-10-25") and len(t) < len(h)
    assert list(h.columns) == list(fio.EMPTY["bottle_history"])


def test_club_orders_take_rate():
    m = make_memberships(); rel = [pd.Timestamp("2026-09-02")]
    o = make_club_orders(m, rel)
    estate_ids = set(m[(m.club_name == "Estate") & (m.status == "Active")].customer_id)
    n_orders = o[o.customer_id.isin(estate_ids)].order_id.nunique()
    assert abs(n_orders / len(estate_ids) - 0.78) < 0.03
    assert o.order_id.nunique() == len(o.groupby("order_id"))


def test_memberships_tiers_dual_and_orders_once():
    m = make_memberships()
    act = m[m.status == "Active"]
    assert act.groupby("club_name").size().to_dict() == TIER_ACTIVE
    assert set(m.columns) == set(fio.EMPTY["memberships"])
    both = set(act[act.club_name == "Estate"].customer_id) & set(act[act.club_name == "Premier 6"].customer_id)
    assert len(both) == 40
    o = make_club_orders(m, [pd.Timestamp("2026-09-02")])
    assert set(o.columns) == set(fio.EMPTY["club_orders"])
    assert set(o.vintage) == {2024}
    per = o.groupby("customer_id").order_id.nunique()
    assert per.max() == 1
    s = make_members_daily(m, "2026-01-01", "2026-10-25")
    assert s.index[0] == pd.Timestamp("2026-01-01") and s.iloc[-1] == len(act)
