"""Synthetic bottle sales, club memberships and club orders with known curves,
tier sizes and take rates. Test fixtures for the v2 bottle and club models;
shapes match the (stubbed) loaders in forecast/io.py."""
from __future__ import annotations

import uuid
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

LABELS = ["Home Ranch Pinot Noir", "Three Hills Pinot Noir", "Carneros Chardonnay",
          "Hypersonic Pinot Noir", "Ten Oaks Pinot Noir", "Mikado Tree Pinot Noir - 1.5L"]
VINTAGES: Dict[str, List[int]] = {
    "Home Ranch Pinot Noir": [2021, 2022, 2023, 2024],
    "Three Hills Pinot Noir": [2021, 2022, 2023, 2024],
    "Carneros Chardonnay": [2021, 2022, 2023, 2024],
    "Hypersonic Pinot Noir": [2023, 2024],
    "Ten Oaks Pinot Noir": [2021, 2022, 2023],
    "Mikado Tree Pinot Noir - 1.5L": [2022],
}
VARIETALS = {l: ("Chardonnay" if "Chardonnay" in l else "Pinot Noir") for l in LABELS}
PRICES = {"Home Ranch Pinot Noir": 62.0, "Three Hills Pinot Noir": 58.0, "Carneros Chardonnay": 48.0,
          "Hypersonic Pinot Noir": 85.0, "Ten Oaks Pinot Noir": 52.0, "Mikado Tree Pinot Noir - 1.5L": 160.0}
# Lifetime bottles for vintage 2021 = 4000 * scale; grows 5% per vintage.
LIFETIME_SCALE = {"Home Ranch Pinot Noir": 1.0, "Three Hills Pinot Noir": 0.6, "Carneros Chardonnay": 0.8,
                  "Hypersonic Pinot Noir": 0.3, "Ten Oaks Pinot Noir": 0.3, "Mikado Tree Pinot Noir - 1.5L": 0.3}
RELEASE_MONTH = {"Hypersonic Pinot Noir": 7}   # default 6 (June of vintage + 2)
CHANNEL_MIX_RELEASE = {"Club": 0.45, "POS": 0.40, "Web": 0.08, "Inbound": 0.07}   # months 3-4 since release
CHANNEL_MIX_OTHER = {"Club": 0.0, "POS": 0.40 / 0.55, "Web": 0.08 / 0.55, "Inbound": 0.07 / 0.55}


def _build_curve() -> np.ndarray:
    """36 monthly shares: first 12 months 48%, next 12 months 41%, last 12 months 11%.
    Within each segment shares decay linearly (first month heaviest)."""
    out = []
    for total in (0.48, 0.41, 0.11):
        w = np.linspace(1.6, 0.4, 12)
        out.append(total * w / w.sum())
    return np.concatenate(out)


CURVE = _build_curve()
# Ten Oaks is the slow label: 80% of its stock is still unsold at lifetime end.
UNSOLD_SHARE = {"Ten Oaks Pinot Noir": 0.8}


def lifetime_bottles(label: str, vintage: int) -> float:
    return 4000.0 * LIFETIME_SCALE[label] * (1.05 ** (vintage - 2021))


def cases_made(label: str, vintage: int) -> int:
    """Cases produced: lifetime sales divided by the sold fraction, 12 bottles a case."""
    sold = 1.0 - UNSOLD_SHARE.get(label, 0.0)
    return int(round(lifetime_bottles(label, vintage) / sold / 12.0))


def make_bottle_history(seed: int = 0, end: str = "2026-10-25", full_lifetime: bool = False) -> pd.DataFrame:
    """Monthly bottles by label x vintage x channel (columns of load_bottle_history).

    Default: history through `end`, no row with month_start after end's month. The last
    month is emitted at full-month volume even though `end` falls mid-month (known
    simplification). full_lifetime=True emits every vintage's complete 36-month curve
    regardless of `end` (includes future months, for curve-shape tests)."""
    rng = np.random.default_rng(seed)
    last_month = pd.Timestamp(end).to_period("M").start_time
    rows = []
    for label in LABELS:
        for vintage in VINTAGES[label]:
            life = lifetime_bottles(label, vintage)
            start = pd.Timestamp(year=vintage + 2, month=RELEASE_MONTH.get(label, 6), day=1)
            for m in range(36):
                month_start = start + pd.DateOffset(months=m)
                if not full_lifetime and month_start > last_month:
                    break
                mix = CHANNEL_MIX_RELEASE if m in (2, 3) else CHANNEL_MIX_OTHER
                for ch, share in mix.items():
                    if share <= 0:
                        continue
                    b = life * CURVE[m] * share * float(rng.normal(1.0, 0.02))
                    b = int(round(b))
                    if b <= 0:
                        continue
                    price = PRICES[label]
                    rows.append({"label": label, "vintage": vintage, "varietal": VARIETALS[label],
                                 "month_start": month_start, "channel": ch, "bottles": b,
                                 "revenue": float(b * price), "price": price})
    cols = ["label", "vintage", "varietal", "month_start", "channel", "bottles", "revenue", "price"]
    return pd.DataFrame(rows, columns=cols)


# --- clubs -------------------------------------------------------------------

TIER_ACTIVE = {"Estate": 1500, "Premier 4": 850, "Grand 4": 350, "Premier 6": 260, "Tiny": 5}
TIER_INACTIVE = {"Estate": 400, "Premier 4": 250, "Grand 4": 80, "Premier 6": 90, "Tiny": 0}
BOX_SIZE = {"Estate": 4, "Premier 4": 4, "Grand 4": 4, "Premier 6": 6}
TAKE_RATE = {"Estate": 0.78, "Premier 4": 0.65, "Grand 4": 0.85, "Premier 6": 0.72}
N_DUAL = 40
TIER_IDS = {t: str(uuid.uuid5(uuid.NAMESPACE_DNS, f"synth.club.{t}")) for t in TIER_ACTIVE}

# ALLOCATION[season][tier] = [(label, vintage, qty, price)]. Vintage is None here and
# resolved per release by `allocation_vintage` (fall: year - 2, others: year - 3).
ALLOCATION: Dict[str, Dict[str, List[Tuple[str, object, int, float]]]] = {}
for _season, _labels in {
    "spring": ["Home Ranch Pinot Noir", "Three Hills Pinot Noir", "Carneros Chardonnay"],
    "summer": ["Carneros Chardonnay", "Three Hills Pinot Noir", "Home Ranch Pinot Noir"],
    "fall": ["Home Ranch Pinot Noir", "Hypersonic Pinot Noir", "Three Hills Pinot Noir"],
    "winter": ["Three Hills Pinot Noir", "Home Ranch Pinot Noir", "Carneros Chardonnay"],
}.items():
    ALLOCATION[_season] = {
        "Estate": [(_labels[0], None, 2, PRICES[_labels[0]] * 0.8), (_labels[1], None, 1, PRICES[_labels[1]] * 0.8),
                   (_labels[2], None, 1, PRICES[_labels[2]] * 0.8)],
        "Premier 4": [(_labels[0], None, 2, PRICES[_labels[0]] * 0.85), (_labels[1], None, 2, PRICES[_labels[1]] * 0.85)],
        "Grand 4": [(_labels[0], None, 1, PRICES[_labels[0]] * 0.9), (_labels[1], None, 1, PRICES[_labels[1]] * 0.9),
                    (_labels[2], None, 2, PRICES[_labels[2]] * 0.9)],
        "Premier 6": [(_labels[0], None, 3, PRICES[_labels[0]] * 0.85), (_labels[1], None, 2, PRICES[_labels[1]] * 0.85),
                      (_labels[2], None, 1, PRICES[_labels[2]] * 0.85)],
    }


def allocation_vintage(season: str, release: pd.Timestamp) -> int:
    return int(release.year) - (2 if season == "fall" else 3)


def _season(d: pd.Timestamp) -> str:
    return ("spring", "summer", "fall", "winter")[(d.month - 1) // 3]


def make_memberships(seed: int = 0) -> pd.DataFrame:
    """One row per membership. 40 customers hold Estate + Premier 6 (both active)."""
    rng = np.random.default_rng(seed)
    lo, hi = pd.Timestamp("2020-01-01"), pd.Timestamp("2026-06-30")
    span = (hi - lo).days
    cutoff = pd.Timestamp("2026-10-20")
    rows = []
    next_cust = [1]
    next_mid = [1]

    def add(tier: str, customer_id: int, active: bool) -> None:
        signup = lo + pd.Timedelta(days=int(rng.integers(0, span)))
        cancel = pd.NaT
        if not active:
            room = max((cutoff - signup).days, 2)
            cancel = signup + pd.Timedelta(days=int(rng.integers(30, max(room, 31))))
            cancel = min(cancel, cutoff)
        rows.append({"membership_id": next_mid[0], "customer_id": customer_id, "club_id": TIER_IDS[tier],
                     "club_name": tier, "status": "Active" if active else "Cancelled",
                     "signup_at": signup, "cancel_at": cancel,
                     "delivery_method": "Ship" if rng.random() < 0.8 else "Pickup"})
        next_mid[0] += 1

    estate_customers = []
    for _ in range(TIER_ACTIVE["Estate"]):
        estate_customers.append(next_cust[0])
        add("Estate", next_cust[0], True)
        next_cust[0] += 1
    for cid in estate_customers[:N_DUAL]:
        add("Premier 6", cid, True)
    for tier, n in TIER_ACTIVE.items():
        n_new = n - (N_DUAL if tier == "Premier 6" else 0) if tier != "Estate" else 0
        for _ in range(n_new):
            add(tier, next_cust[0], True)
            next_cust[0] += 1
    for tier, n in TIER_INACTIVE.items():
        for _ in range(n):
            add(tier, next_cust[0], False)
            next_cust[0] += 1
    cols = ["membership_id", "customer_id", "club_id", "club_name", "status", "signup_at",
            "cancel_at", "delivery_method"]
    return pd.DataFrame(rows, columns=cols)


def make_club_orders(memberships: pd.DataFrame, releases: List[pd.Timestamp], seed: int = 0) -> pd.DataFrame:
    """Club orders: per release and tier, round(take_rate * eligible members) place one order
    on release day + 1. Dual-membership customers order once, with the Premier 6 box."""
    rng = np.random.default_rng(seed)
    dual = set(memberships[memberships.club_name == "Premier 6"].customer_id) & \
        set(memberships[memberships.club_name == "Estate"].customer_id)
    rows = []
    for rel in releases:
        rel = pd.Timestamp(rel)
        season = _season(rel)
        vintage = allocation_vintage(season, rel)
        elig = memberships[(memberships.signup_at <= rel) &
                           (memberships.cancel_at.isna() | (memberships.cancel_at > rel))]
        for tier, rate in TAKE_RATE.items():
            pool = elig[elig.club_name == tier]
            custs = sorted(set(pool.customer_id) - (dual if tier == "Estate" else set()))
            n = int(round(rate * len(custs)))
            if n == 0:
                continue
            chosen = rng.choice(np.array(custs), size=n, replace=False)
            lines = ALLOCATION[season][tier]
            for cid in chosen:
                oid = f"{rel:%Y%m%d}-{int(cid)}"
                for label, _v, qty, price in lines:
                    rows.append({"order_id": oid, "customer_id": int(cid),
                                 "created_at": rel + pd.Timedelta(days=1), "label": label,
                                 "vintage": vintage, "bottles": qty, "revenue": float(qty * price)})
    cols = ["order_id", "customer_id", "created_at", "label", "vintage", "bottles", "revenue"]
    return pd.DataFrame(rows, columns=cols)


def make_members_daily(memberships: pd.DataFrame, start, end) -> pd.Series:
    """Active memberships per day (one per membership row), indexed by date."""
    idx = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq="D")
    signups = memberships.signup_at.dt.normalize().value_counts()
    cancels = memberships.cancel_at.dropna().dt.normalize().value_counts()
    ev = signups.reindex(idx, fill_value=0).astype(float) - cancels.reindex(idx, fill_value=0).astype(float)
    base_signed = float((memberships.signup_at < idx[0]).sum())
    base_cancel = float((memberships.cancel_at.notna() & (memberships.cancel_at < idx[0])).sum())
    return (ev.cumsum() + base_signed - base_cancel).rename("members")
