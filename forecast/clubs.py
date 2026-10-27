"""Club release history, tier shares and take-rate defaults (spec 6.1-6.2). Pure pandas, no IO."""
from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from forecast.config import EngineParams, season_of

HISTORY_COLUMNS = ["release_start", "season", "tier_id", "tier_name", "label", "vintage", "qty_per_box",
                   "price", "orders", "members", "take_rate", "bottles", "revenue"]
OTHER = "other"
PRODUCT_MIN_SHARE = 0.05


def active_on(memberships: pd.DataFrame, date) -> pd.DataFrame:
    """Memberships signed up on/before `date` and still active then. A null cancel_at with a
    non-Active status counts as inactive."""
    date = pd.Timestamp(date)
    m = memberships
    live = (m.status == "Active") | (m.cancel_at.notna() & (m.cancel_at > date))
    return m[(m.signup_at <= date) & live]


def tier_table(memberships: pd.DataFrame, origin, min_members: int) -> pd.DataFrame:
    """Tiers (grouped by club_id) with at least `min_members` active at origin; share of the listed
    active total. tier_name = the most recent non-null club_name for the id by signup_at among
    memberships signed up on/before origin, else the id as a string (a null name never drops a club)."""
    cols = ["tier_id", "tier_name", "active", "share"]
    act = active_on(memberships, origin)
    if act.empty:
        return pd.DataFrame(columns=cols)
    g = act.groupby("club_id").size().reset_index(name="active")
    g = g[g.active >= min_members]
    if g.empty:
        return pd.DataFrame(columns=cols)
    named = memberships[(memberships.signup_at <= pd.Timestamp(origin)) & memberships.club_name.notna()]
    names = named.sort_values("signup_at", kind="stable").groupby("club_id").club_name.last()
    g = g.rename(columns={"club_id": "tier_id"})
    g["tier_name"] = [names.get(t, str(t)) for t in g.tier_id]
    g["share"] = g.active / g.active.sum()
    return g.sort_values("active", ascending=False).reset_index(drop=True)[cols]


def attribute_orders(orders: pd.DataFrame, memberships: pd.DataFrame, release_date, tiers: pd.DataFrame) -> pd.DataFrame:
    """Add `tier_id` to each order line; "other" when the customer has no active listed membership.
    Pass 1: customers with one listed tier. Pass 2: multi-tier customers go to the tier whose
    pass-1 median bottles-per-order is nearest (ties: most recent signup)."""
    out = orders.copy()
    if out.empty:
        out["tier_id"] = pd.Series(dtype=object)
        return out
    act = active_on(memberships, release_date)
    act = act[act.club_id.isin(set(tiers.tier_id))]
    n_tiers = act.groupby("customer_id").club_id.nunique()
    single = n_tiers[n_tiers == 1].index
    single_tier = act[act.customer_id.isin(single)].drop_duplicates("customer_id").set_index("customer_id").club_id
    size = out.groupby("order_id").bottles.sum()
    cust = out.groupby("order_id").customer_id.first()
    o1 = pd.DataFrame({"size": size, "tier": cust.map(single_tier)}).dropna(subset=["tier"])
    medians = o1.groupby("tier")["size"].median()
    multi = n_tiers[n_tiers > 1].index
    recent = act[act.customer_id.isin(multi)].sort_values("signup_at", ascending=False)
    cand = {c: list(g.club_id.drop_duplicates()) for c, g in recent.groupby("customer_id")}

    def pick(oid: str) -> object:
        c = cust[oid]
        if c in single_tier.index:
            return single_tier[c]
        opts = cand.get(c)
        if not opts:
            return OTHER
        known = [t for t in opts if t in medians.index]
        if not known:
            return opts[0]
        # min is stable: ties resolve to the earliest option, i.e. most recent signup
        return min(known, key=lambda t: abs(medians[t] - size[oid]))

    tier_of = pd.Series({oid: pick(oid) for oid in size.index})
    out["tier_id"] = out.order_id.map(tier_of)
    return out


def release_history(orders_by_release: Dict[pd.Timestamp, pd.DataFrame], memberships: pd.DataFrame,
                    members_daily, tiers: pd.DataFrame, params: EngineParams) -> pd.DataFrame:
    """Per release x tier x product history. `members_daily` and `params` are accepted for
    signature parity with the loaders and are unused: members come from `active_on`."""
    names = dict(zip(tiers.tier_id, tiers.tier_name))
    names[OTHER] = "Other"
    rows = []
    for rel in sorted(orders_by_release):
        rel_ts = pd.Timestamp(rel)
        o = orders_by_release[rel]
        if o is None or o.empty:
            continue
        a = attribute_orders(o, memberships, rel_ts, tiers)
        act = active_on(memberships, rel_ts)
        members = act.groupby("club_id").size()
        for tid, t in a.groupby("tier_id"):
            n_orders = int(t.order_id.nunique())
            n_mem = 0 if tid == OTHER else int(members.get(tid, 0))
            take = np.nan if tid == OTHER or n_mem == 0 else n_orders / n_mem
            tier_bottles = float(t.bottles.sum())
            for (label, vintage), p in t.groupby(["label", "vintage"], dropna=False):
                b = float(p.bottles.sum())
                if tier_bottles <= 0 or b < PRODUCT_MIN_SHARE * tier_bottles:
                    continue
                rev = float(p.revenue.sum())
                rows.append({"release_start": rel_ts, "season": season_of(rel_ts), "tier_id": tid,
                             "tier_name": names.get(tid, str(tid)), "label": label, "vintage": vintage,
                             "qty_per_box": max(1, int(round(b / n_orders))),
                             "price": rev / b if b else np.nan, "orders": n_orders, "members": n_mem,
                             "take_rate": take, "bottles": int(b), "revenue": rev})
    return pd.DataFrame(rows, columns=HISTORY_COLUMNS)


def take_rate_default(history: pd.DataFrame, tier_id: str, season: str) -> float:
    """Linear trend over the last four same-season releases, one step ahead, clipped to [0.3, 1.0]."""
    if history is None or history.empty:
        return 0.6
    h = history[(history.tier_id == tier_id) & (history.season == season)]
    h = h.drop_duplicates("release_start").sort_values("release_start").dropna(subset=["take_rate"])
    y = h.take_rate.to_numpy(dtype=float)[-4:]
    if len(y) == 0:
        return 0.6
    if len(y) < 2:
        return float(np.clip(y.mean(), 0.3, 1.0))
    slope, icpt = np.polyfit(np.arange(len(y)), y, 1)
    return float(np.clip(slope * len(y) + icpt, 0.3, 1.0))
