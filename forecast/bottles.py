"""Bottle label parsing, per-vintage table and price tiers (spec 5.1, 5.3). Pure pandas, no IO."""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from forecast.config import BOTTLE_CHANNELS, EngineParams

log = logging.getLogger("forecast")

MAGNUM_SUFFIX = " - 1.5L"
_VINTAGE_PREFIX = re.compile(r"^\d{4}\s+")
_VINTAGE_DIGITS = re.compile(r"^(\d{4})")


def label_from_title(title: str) -> str:
    """Title with the leading four-digit vintage stripped (magnum suffix is kept)."""
    return _VINTAGE_PREFIX.sub("", title.strip())


def vintage_from_title(title: str) -> Optional[int]:
    m = _VINTAGE_DIGITS.match(title.strip())
    return int(m.group(1)) if m else None


def varietal_from_title(title: str) -> Optional[str]:
    """Varietal from a title: strip the leading vintage and any ' - 1.5L' suffix; if the rest ends
    with 'pinot noir' (case-insensitive) return 'Pinot Noir', else the last word (original casing);
    None when no words remain. A fallback only: callers prefer the warehouse stg_product.varietal."""
    t = label_from_title(title)
    if t.endswith(MAGNUM_SUFFIX):
        t = t[: -len(MAGNUM_SUFFIX)]
    t = _VINTAGE_DIGITS.sub("", t).strip() if t.strip().isdigit() else t.strip()
    if not t:
        return None
    if t.lower().endswith("pinot noir"):
        return "Pinot Noir"
    return t.split()[-1]


def is_magnum(title: str) -> bool:
    return title.strip().endswith(MAGNUM_SUFFIX)


VINTAGE_COLUMNS = ["label", "vintage", "varietal", "release_month", "sold_to_date", "months_sold", "first_year"]


def vintage_table(hist: pd.DataFrame, origin: pd.Timestamp, threshold: int = 50) -> pd.DataFrame:
    """One row per label-vintage with sales on or before origin. release_month is the first month
    with >= threshold bottles (all channels), else the first month with any sales. first_year is the
    bottles in the 12 months from release_month, NaN when months_sold < 12."""
    origin_month = pd.Timestamp(origin).to_period("M").start_time
    h = hist[hist["month_start"] <= origin]
    rows = []
    for (label, vintage), g in h.groupby(["label", "vintage"], sort=True):
        monthly = g.groupby("month_start")["bottles"].sum().sort_index()
        if monthly.empty:
            continue
        big = monthly[monthly >= threshold]
        release = (big.index[0] if len(big) else monthly.index[0])
        months_sold = (origin_month.year - release.year) * 12 + origin_month.month - release.month + 1
        end = release + pd.DateOffset(months=12)
        first_year = float(monthly[(monthly.index >= release) & (monthly.index < end)].sum()) \
            if months_sold >= 12 else np.nan
        vs = g["varietal"].dropna() if "varietal" in g else pd.Series(dtype=object)
        varietal = vs.iloc[0] if len(vs) else varietal_from_title(str(label))  # label has no vintage; may be None
        rows.append({"label": label, "vintage": int(vintage), "varietal": varietal, "release_month": release,
                     "sold_to_date": float(monthly.sum()), "months_sold": int(months_sold),
                     "first_year": first_year})
    return pd.DataFrame(rows, columns=VINTAGE_COLUMNS)


def price_bands(hist: pd.DataFrame) -> Dict[str, Tuple[float, float]]:
    """Tercile cut points (t1, t2) of distinct labels' median prices within each varietal. A varietal
    with fewer than 3 labels gets (median, median)."""
    per = hist.groupby("label").agg(price=("price", "median"), varietal=("varietal", "first")).reset_index()
    per["varietal"] = per["varietal"].where(per["varietal"].notna(), per["label"].map(varietal_from_title))
    out: Dict[str, Tuple[float, float]] = {}
    for varietal, g in per.dropna(subset=["varietal"]).groupby("varietal"):
        p = g["price"].astype(float)
        if len(p) < 3:
            med = float(p.median())
            out[varietal] = (med, med)
        else:
            out[varietal] = (float(p.quantile(1 / 3)), float(p.quantile(2 / 3)))
    return out


def tier_key(varietal: str, price: float, bands: Dict[str, Tuple[float, float]]) -> str:
    t1, t2 = bands[varietal]
    band = "low" if price <= t1 else ("high" if price > t2 else "mid")
    return f"{varietal}|{band}"


# --- profiles and label-vintage forecast (spec 5.3) ----------------------------

PROFILE_COLUMNS = ["scope", "key", "channel", "curve", "lifetime", "trend", "log_dispersion", "n_vintages"]
FORECAST_COLUMNS = ["label", "vintage", "month_start", "channel", "p50", "p10", "p90"]
SUMMARY_COLUMNS = ["label", "vintage", "varietal", "release_month", "sold_to_date", "cases_made",
                   "lifetime_p50", "lifetime_p10", "lifetime_p90", "profile_key"]
QUALIFY_MONTHS = 24            # a vintage feeds a profile once it has sold this many months
DEFAULT_LOG_DISPERSION = 0.35
MIN_DISPERSION_ROWS = 3
NEW_LABEL_SELL_THROUGH = 0.85  # share of planned bottles a brand-new label is expected to sell
Z90 = 1.2816


def _channels(hist: pd.DataFrame) -> List[str]:
    seen = set(hist["channel"].dropna().unique()) if "channel" in hist else set()
    return list(BOTTLE_CHANNELS) + sorted(str(c) for c in seen - set(BOTTLE_CHANNELS))


def _month_diff(a: pd.Timestamp, b: pd.Timestamp) -> int:
    """Whole months from a to b."""
    return (b.year - a.year) * 12 + b.month - a.month


def _label_prices(hist: pd.DataFrame) -> Dict[str, float]:
    if hist.empty or "price" not in hist:
        return {}
    return {k: float(v) for k, v in hist.groupby("label")["price"].median().items()}


def _safe_tier(varietal, price, bands) -> Optional[str]:
    if varietal is None or price is None or pd.isna(varietal) or pd.isna(price):
        return None
    try:
        return tier_key(varietal, float(price), bands)
    except (KeyError, TypeError, ValueError):
        return None


def _log_ratios(dispersion: Optional[pd.DataFrame]) -> Dict[str, List[float]]:
    """log(first_year_actual / first_year_forecast) per label, positive pairs only."""
    if dispersion is None or dispersion.empty:
        return {}
    d = dispersion.copy()
    a = pd.to_numeric(d["first_year_actual"], errors="coerce")
    f = pd.to_numeric(d["first_year_forecast"], errors="coerce")
    d = d[(a > 0) & (f > 0)]
    lr = np.log(a[d.index] / f[d.index])
    out: Dict[str, List[float]] = {}
    for label, x in zip(d["label"], lr):
        if np.isfinite(x):
            out.setdefault(label, []).append(float(x))
    return out


def _std_or_none(xs: List[float]) -> Optional[float]:
    return float(np.std(xs, ddof=1)) if len(xs) >= MIN_DISPERSION_ROWS else None


def _normalise(curve: np.ndarray) -> np.ndarray:
    tot = curve.sum()
    return curve / tot if tot > 0 else curve


def fit_profiles(hist: pd.DataFrame, vintages: pd.DataFrame, params: EngineParams,
                 dispersion: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Sell-through profiles per label and per varietal-price tier.

    A vintage qualifies when months_sold >= 24. Its curve is bottles by month-since-release and
    channel (n = bottle_curve_months slots, only months through origin, missing = 0) divided by its
    all-channel total over those slots. A label's curve is the equal-weight mean of its qualifying
    vintages' curves; a tier's is the mean over every qualifying label-vintage of the tier. Curves
    are renormalised so a key's channel curves sum to 1. Label lifetime = latest qualifying
    vintage's slot total, extrapolated through the tier curve (label curve if no tier) when it has
    fewer than n months; tier lifetime = mean of its labels' lifetimes. Label trend =
    1 + lambda * (clip(fy[latest] / fy[previous]) - 1) over the two latest vintages with a
    first_year; tier trend = 1. log_dispersion = sample std of log(actual / forecast) first-year
    rows for the label (>= 3), else the tier's (>= 3), else 0.35."""
    n = int(params.bottle_curve_months)
    channels = _channels(hist)
    if vintages is None or vintages.empty or hist.empty:
        return pd.DataFrame(columns=PROFILE_COLUMNS)
    qual = vintages[vintages["months_sold"] >= QUALIFY_MONTHS]
    meta = qual.set_index(["label", "vintage"])[["release_month", "months_sold"]]

    # Per qualifying vintage: (channels x n) bottle slots through origin.
    h = hist.merge(qual[["label", "vintage", "release_month", "months_sold"]], on=["label", "vintage"])
    slots: Dict[Tuple[str, int], np.ndarray] = {}
    if not h.empty:
        rel = pd.DatetimeIndex(h["release_month"])
        ms = pd.DatetimeIndex(h["month_start"])
        h = h.assign(m=(ms.year - rel.year) * 12 + (ms.month - rel.month))
        h = h[(h["m"] >= 0) & (h["m"] < np.minimum(h["months_sold"], n))]
        ch_idx = {c: i for i, c in enumerate(channels)}
        for (label, vintage), g in h.groupby(["label", "vintage"], sort=True):
            arr = np.zeros((len(channels), n))
            np.add.at(arr, (g["channel"].map(ch_idx).to_numpy(), g["m"].to_numpy()),
                      g["bottles"].astype(float).to_numpy())
            slots[(label, int(vintage))] = arr
    curves = {k: a / a.sum() for k, a in slots.items() if a.sum() > 0}

    prices = _label_prices(hist)
    bands = price_bands(hist)
    varietals = {lab: (g["varietal"].dropna().iloc[0] if g["varietal"].notna().any() else None)
                 for lab, g in vintages.groupby("label")}
    labels = sorted({lab for lab, _v in curves})
    tier_of = {lab: _safe_tier(varietals.get(lab), prices.get(lab), bands) for lab in labels}

    tier_curves: Dict[str, np.ndarray] = {}
    tier_n: Dict[str, int] = {}
    for tk in sorted({t for t in tier_of.values() if t is not None}):
        members = [c for (lab, _v), c in curves.items() if tier_of[lab] == tk]
        tier_curves[tk] = _normalise(np.mean(members, axis=0))
        tier_n[tk] = len(members)

    ratios = _log_ratios(dispersion)
    tier_ratios: Dict[str, List[float]] = {}
    for lab, xs in ratios.items():
        tk = _safe_tier(varietals.get(lab), prices.get(lab), bands)
        if tk is not None:
            tier_ratios.setdefault(tk, []).extend(xs)
    lo, hi = params.trend_clip

    rows = []
    label_life: Dict[str, float] = {}
    for lab in labels:
        keys = sorted(v for (l2, v) in curves if l2 == lab)
        curve = _normalise(np.mean([curves[(lab, v)] for v in keys], axis=0))
        latest = keys[-1]
        total = float(slots[(lab, latest)].sum())
        msold = int(meta.loc[(lab, latest), "months_sold"])
        if msold < n:
            ref = tier_curves.get(tier_of[lab], curve)
            cum = float(ref[:, :msold].sum())
            if cum > 0:
                total = total / cum
        label_life[lab] = total
        fy = vintages[(vintages["label"] == lab) & vintages["first_year"].notna()].sort_values("vintage")
        trend = 1.0
        if len(fy) >= 2:
            prev, last = float(fy["first_year"].iloc[-2]), float(fy["first_year"].iloc[-1])
            if prev > 0:
                trend = 1.0 + params.lambda_shrink * (float(np.clip(last / prev, lo, hi)) - 1.0)
        disp = _std_or_none(ratios.get(lab, []))
        if disp is None and tier_of[lab] is not None:
            disp = _std_or_none(tier_ratios.get(tier_of[lab], []))
        disp = DEFAULT_LOG_DISPERSION if disp is None else disp
        rows += _profile_rows("label", lab, channels, curve, total, trend, disp, len(keys))
    for tk, curve in tier_curves.items():
        life = float(np.mean([label_life[lab] for lab in labels if tier_of[lab] == tk]))
        disp = _std_or_none(tier_ratios.get(tk, []))
        disp = DEFAULT_LOG_DISPERSION if disp is None else disp
        rows += _profile_rows("tier", tk, channels, curve, life, 1.0, disp, tier_n[tk])
    return pd.DataFrame(rows, columns=PROFILE_COLUMNS)


def _profile_rows(scope: str, key: str, channels: List[str], curve: np.ndarray, lifetime: float,
                  trend: float, disp: float, n_vintages: int) -> List[dict]:
    return [{"scope": scope, "key": key, "channel": ch, "curve": [float(x) for x in curve[i]],
             "lifetime": float(lifetime), "trend": float(trend), "log_dispersion": float(disp),
             "n_vintages": int(n_vintages)} for i, ch in enumerate(channels)]


def profile_for(profiles: pd.DataFrame, label: str, varietal, price, comparable: Optional[str],
                bands: Dict[str, Tuple[float, float]]) -> Optional[pd.DataFrame]:
    """Fallback chain: the label's own rows, else the comparable label's rows, else the tier rows
    for (varietal, price), else None (also None for an unknown varietal or missing price)."""
    if profiles is None or profiles.empty:
        return None
    lab = profiles[profiles["scope"] == "label"]
    for name in (label, comparable):
        if isinstance(name, str) and name:
            rows = lab[lab["key"] == name]
            if len(rows):
                return rows
    tk = _safe_tier(varietal, price, bands)
    if tk is None:
        return None
    rows = profiles[(profiles["scope"] == "tier") & (profiles["key"] == tk)]
    return rows if len(rows) else None


def _plan_value(row: Optional[pd.Series], col: str):
    if row is None or col not in row.index:
        return None
    v = row[col]
    return None if v is None or (not isinstance(v, str) and pd.isna(v)) else v


def _default_release(label: str, vintage: int, vintages: pd.DataFrame, plan_year: int) -> pd.Timestamp:
    """Modal release month-of-year over the label's vintages (ties: the most recent vintage's),
    in the year in which the previous vintage's release + 12 months falls; else June of plan_year."""
    own = vintages[vintages["label"] == label].sort_values("vintage")
    prev = own[own["vintage"] < vintage]
    if own.empty or prev.empty:
        return pd.Timestamp(year=plan_year, month=6, day=1)
    months = [int(pd.Timestamp(r).month) for r in own["release_month"]]
    counts = pd.Series(months).value_counts()
    tied = set(counts[counts == counts.max()].index)
    month = next(m for m in reversed(months) if m in tied)
    year = (pd.Timestamp(prev["release_month"].iloc[-1]) + pd.DateOffset(months=12)).year
    return pd.Timestamp(year=int(year), month=month, day=1)


def forecast_vintages(profiles: pd.DataFrame, vintages: pd.DataFrame, hist: pd.DataFrame,
                      label_plan: pd.DataFrame, vintage_plan: pd.DataFrame, origin: pd.Timestamp,
                      plan_year: int, params: EngineParams,
                      bands: Dict[str, Tuple[float, float]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Monthly p50/p10/p90 by label-vintage-channel, plus a per-vintage summary.

    Candidates: vintages with months_sold < n, plus each continue/new label's incoming vintage
    (default newest on record + 1 for continue, plan_year - 2 for new). Lifetime: existing label =
    profile lifetime x profile trend; new label = cases x 12 x 0.85, or x min(1, comparable
    lifetime / (comparable cases x 12)) when the comparable label has its own profile and cases
    for its latest qualifying vintage; with no cases on file, the profile lifetime x trend. Capped
    at cases x 12 when cases are known. An already-selling vintage spreads max(lifetime -
    sold_to_date, 0) over the curve mass from months_sold on; a future one is lifetime x curve.
    Rows run from the month after origin to December of plan_year + 1."""
    n = int(params.bottle_curve_months)
    origin_month = pd.Timestamp(origin).to_period("M").start_time
    window_end = pd.Timestamp(year=plan_year + 1, month=12, day=1)
    vintages = vintages if vintages is not None else pd.DataFrame(columns=VINTAGE_COLUMNS)
    vt = {(r.label, int(r.vintage)): r for r in vintages.itertuples(index=False)}
    plan: Dict[str, pd.Series] = {}
    if label_plan is not None:
        for _, r in label_plan.iterrows():
            plan[r["label"]] = r
    cases: Dict[Tuple[str, int], float] = {}
    if vintage_plan is not None:
        for r in vintage_plan.itertuples(index=False):
            c = pd.to_numeric(r.cases_made, errors="coerce")
            cases[(r.label, int(r.vintage))] = float(c) if pd.notna(c) else np.nan
    prices = _label_prices(hist)
    lab_varietal = {lab: (g["varietal"].dropna().iloc[0] if g["varietal"].notna().any() else None)
                    for lab, g in vintages.groupby("label")}

    cands: List[Tuple[str, int]] = [(r.label, int(r.vintage)) for r in vintages.itertuples(index=False)
                                    if r.months_sold < n]
    for label, r in plan.items():
        status = _plan_value(r, "status")
        if status not in ("continue", "new"):
            continue
        inc = _plan_value(r, "incoming_vintage")
        if inc is None:
            own = vintages[vintages["label"] == label]["vintage"]
            inc = int(own.max()) + 1 if status == "continue" and len(own) else plan_year - 2
        cands.append((label, int(inc)))
    cands = list(dict.fromkeys(cands))

    fc_rows: List[dict] = []
    summ_rows: List[dict] = []
    for label, vintage in cands:
        pr = plan.get(label)
        is_new = _plan_value(pr, "status") == "new"
        v = vt.get((label, vintage))
        varietal = lab_varietal.get(label) or _plan_value(pr, "varietal") or varietal_from_title(label)
        price = _plan_value(pr, "price")
        price = prices.get(label) if price is None else float(price)
        comparable = _plan_value(pr, "comparable_label")
        prof = profile_for(profiles, label, varietal, price, comparable, bands)
        if prof is None and not is_new:
            log.warning("bottles: no profile for %s %s (varietal %s); skipped", label, vintage, varietal)
            continue
        if v is not None:
            release = pd.Timestamp(v.release_month)
        else:
            rm = _plan_value(pr, "release_month")
            release = (pd.Timestamp(rm).to_period("M").start_time if rm is not None
                       else _default_release(label, vintage, vintages, plan_year))
        c = cases.get((label, vintage), np.nan)
        sold = float(v.sold_to_date) if v is not None else 0.0

        p_life = float(prof["lifetime"].iloc[0]) if prof is not None else np.nan
        p_trend = float(prof["trend"].iloc[0]) if prof is not None else 1.0
        disp = float(prof["log_dispersion"].iloc[0]) if prof is not None else DEFAULT_LOG_DISPERSION
        if is_new and pd.notna(c):
            factor = NEW_LABEL_SELL_THROUGH
            if isinstance(comparable, str) and prof is not None and prof["scope"].iloc[0] == "label" \
                    and prof["key"].iloc[0] == comparable:
                cq = vintages[(vintages["label"] == comparable) & (vintages["months_sold"] >= QUALIFY_MONTHS)]
                cc = cases.get((comparable, int(cq["vintage"].max())), np.nan) if len(cq) else np.nan
                if pd.notna(cc) and cc > 0:
                    factor = min(1.0, p_life / (cc * 12.0))
            life = float(c) * 12.0 * factor
        else:
            life = p_life * p_trend
        if pd.notna(c):
            life = min(life, float(c) * 12.0)
        life = 0.0 if not np.isfinite(life) else max(life, 0.0)

        if prof is None:
            log.warning("bottles: no profile for new label %s %s; summary only", label, vintage)
        else:
            curve = np.array(list(prof["curve"]), dtype=float)   # channels x n, in prof row order
            if v is not None:
                start = int(v.months_sold)
                remaining = max(life - sold, 0.0)
                mass = float(curve[:, start:].sum())
                monthly = np.zeros_like(curve)
                if mass > 0:
                    monthly[:, start:] = remaining * curve[:, start:] / mass
                life = sold + (remaining if mass > 0 else 0.0)
            else:
                start = 0
                monthly = life * curve
            lo_k, hi_k = np.exp(-Z90 * disp), np.exp(Z90 * disp)
            for m in range(start, n):
                month = release + pd.DateOffset(months=m)
                if month <= origin_month or month > window_end:
                    continue
                for i, ch in enumerate(prof["channel"]):
                    p50 = float(monthly[i, m])
                    fc_rows.append({"label": label, "vintage": vintage, "month_start": month, "channel": ch,
                                    "p50": p50, "p10": p50 * lo_k, "p90": p50 * hi_k})
        summ_rows.append({"label": label, "vintage": vintage, "varietal": varietal, "release_month": release,
                          "sold_to_date": sold, "cases_made": c, "lifetime_p50": life,
                          "lifetime_p10": life * np.exp(-Z90 * disp), "lifetime_p90": life * np.exp(Z90 * disp),
                          "profile_key": (prof["key"].iloc[0] if prof is not None else None)})
    fc = pd.DataFrame(fc_rows, columns=FORECAST_COLUMNS)
    summ = pd.DataFrame(summ_rows, columns=SUMMARY_COLUMNS)
    summ["cases_made"] = summ["cases_made"].astype(float)
    return fc, summ
