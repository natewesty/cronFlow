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
    with fewer than 3 labels gets (median, median). Magnum labels (title ends ' - 1.5L') are excluded."""
    per = hist.groupby("label").agg(price=("price", "median"), varietal=("varietal", "first")).reset_index()
    per = per[~per["label"].astype(str).map(is_magnum)]
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


def _anchor_vintage(vintages: pd.DataFrame, label: str) -> Optional[int]:
    """Newest vintage of the label with a known first_year (None if there is none)."""
    fy = vintages[(vintages["label"] == label) & vintages["first_year"].notna()]
    return int(fy["vintage"].max()) if len(fy) else None


def _complete_curve(slots: np.ndarray, months_sold: int,
                    refs) -> Tuple[np.ndarray, float]:
    """Curve and lifetime estimate of an incomplete vintage (months_sold < n), from a reference shape.

    `refs` is an ordered list of candidate reference curves (None entries skipped; a single array
    is accepted). The first with cum = ref[:, :months_sold].sum() (all channels) > 0 is used:
    lifetime_est = total_to_date / cum; observed months get slots / lifetime_est; months >=
    months_sold get ref * (1 - observed_cum) / ref_remaining. If no reference qualifies, the vintage
    is normalised by its own total to date (lifetime_est = total to date)."""
    total = float(slots.sum())
    if refs is None or isinstance(refs, np.ndarray):
        refs = [refs]
    ref, cum = None, 0.0
    for r in refs:
        if r is not None and float(r[:, :months_sold].sum()) > 0:
            ref, cum = r, float(r[:, :months_sold].sum())
            break
    if ref is None:
        return slots / total, total
    est = total / cum
    out = np.zeros_like(slots)
    out[:, :months_sold] = slots[:, :months_sold] / est
    observed = float(out.sum())
    rem = float(ref[:, months_sold:].sum())
    if rem > 0:
        out[:, months_sold:] = ref[:, months_sold:] * (1.0 - observed) / rem
    return out, est


def fit_profiles(hist: pd.DataFrame, vintages: pd.DataFrame, params: EngineParams,
                 dispersion: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Sell-through profiles per label and per varietal-price tier. n = params.bottle_curve_months.

    Slots: a qualifying vintage (months_sold >= 24) contributes bottles by month-since-release m and
    channel, for 0 <= m < min(months_sold, n) (months through origin only; missing = 0). A vintage
    with an all-channel slot total <= 0 is ignored.

    Vintage curve (channels x n, sums to 1 over channels and months):
      - complete (months_sold >= n): slots / slot total.
      - incomplete (24 <= months_sold < n): completed from a reference shape, see _complete_curve.
        Reference = the tier's reference curve (normalised equal-weight mean of the complete
        vintages' curves of every non-magnum label in the tier); if none, the normalised mean of the
        label's own complete vintages' curves; if neither, own-total normalisation. A reference
        whose cumulative share through months_sold is <= 0 is passed over for the next one.
    Label curve = normalised equal-weight mean of its qualifying vintages' curves. Tier curve =
    normalised equal-weight mean over every qualifying label-vintage of its non-magnum labels.
    Tier of a label = tier_key(first non-null varietal in `vintages`, median hist price,
    price_bands(hist)); magnum labels have no tier.

    Label lifetime = lifetime_est(anchor) = first_year(anchor) / first12_share, where anchor = newest
    vintage of the label with a known first_year and first12_share = label curve summed over months
    0-11 and all channels. If first12_share <= 0, the lifetime estimate of the latest qualifying
    vintage (its slot total if complete, else _complete_curve's estimate) is used instead, and
    consumers then apply no trend compounding (gap 0; detectable from the stored curve).
    Tier lifetime = mean of its labels' lifetimes.
    Label trend = 1 + lambda_shrink * (clip(fy[newest] / fy[previous], *trend_clip) - 1) over the
    two newest vintages with a first_year (1.0 if fewer than two or fy[previous] <= 0); tier trend
    = 1.0. A consumer forecasts vintage V of the profile's label as lifetime * trend ** (V - anchor).
    log_dispersion = sample std (ddof=1) of log(first_year_actual / first_year_forecast) over the
    `dispersion` rows with both values > 0: the label's rows when >= 3, else (non-magnum only) the
    rows of every label in its tier when >= 3, else 0.35. Tier rows use the tier's rows, else 0.35.
    n_vintages = qualifying vintages (label) or label-vintages (tier). Every key has one row per
    channel; a key with no qualifying vintage is absent."""
    n = int(params.bottle_curve_months)
    channels = _channels(hist)
    if vintages is None or vintages.empty or hist.empty:
        return pd.DataFrame(columns=PROFILE_COLUMNS)
    qual = vintages[vintages["months_sold"] >= QUALIFY_MONTHS]
    ms_of = {(r.label, int(r.vintage)): int(r.months_sold) for r in qual.itertuples(index=False)}

    # Per qualifying vintage: (channels x n) bottle slots through origin.
    h = hist.merge(qual[["label", "vintage", "release_month", "months_sold"]], on=["label", "vintage"])
    raw: Dict[Tuple[str, int], np.ndarray] = {}
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
            if arr.sum() > 0:
                raw[(label, int(vintage))] = arr

    prices = _label_prices(hist)
    bands = price_bands(hist)
    varietals = {lab: (g["varietal"].dropna().iloc[0] if g["varietal"].notna().any() else None)
                 for lab, g in vintages.groupby("label")}
    labels = sorted({lab for lab, _v in raw})

    def tier_for(lab: str) -> Optional[str]:
        return None if is_magnum(str(lab)) else _safe_tier(varietals.get(lab), prices.get(lab), bands)

    tier_of = {lab: tier_for(lab) for lab in labels}
    tiers = sorted({t for t in tier_of.values() if t is not None})

    complete = {k: a / a.sum() for k, a in raw.items() if ms_of[k] >= n}
    tier_ref: Dict[str, np.ndarray] = {}
    for tk in tiers:
        members = [c for (lab, _v), c in complete.items() if tier_of[lab] == tk]
        if members:
            tier_ref[tk] = _normalise(np.mean(members, axis=0))
    label_ref = {lab: _normalise(np.mean([c for (l2, _v), c in complete.items() if l2 == lab], axis=0))
                 for lab in labels if any(l2 == lab for (l2, _v) in complete)}

    curves: Dict[Tuple[str, int], np.ndarray] = {}
    life_est: Dict[Tuple[str, int], float] = {}
    for (lab, v), a in raw.items():
        if (lab, v) in complete:
            curves[(lab, v)], life_est[(lab, v)] = complete[(lab, v)], float(a.sum())
            continue
        refs = [tier_ref.get(tier_of[lab]) if tier_of[lab] is not None else None, label_ref.get(lab)]
        curves[(lab, v)], life_est[(lab, v)] = _complete_curve(a, ms_of[(lab, v)], refs)

    tier_curves: Dict[str, np.ndarray] = {}
    tier_n: Dict[str, int] = {}
    for tk in tiers:
        members = [c for (lab, _v), c in curves.items() if tier_of[lab] == tk]
        tier_curves[tk] = _normalise(np.mean(members, axis=0))
        tier_n[tk] = len(members)

    ratios = _log_ratios(dispersion)
    tier_ratios: Dict[str, List[float]] = {}
    for lab, xs in ratios.items():
        tk = tier_for(lab)
        if tk is not None:
            tier_ratios.setdefault(tk, []).extend(xs)
    lo, hi = params.trend_clip
    fy_of = {(r.label, int(r.vintage)): float(r.first_year) for r in vintages.itertuples(index=False)
             if pd.notna(r.first_year)}

    rows = []
    label_life: Dict[str, float] = {}
    for lab in labels:
        keys = sorted(v for (l2, v) in curves if l2 == lab)
        curve = _normalise(np.mean([curves[(lab, v)] for v in keys], axis=0))
        anchor = _anchor_vintage(vintages, lab)
        f12 = float(curve[:, :12].sum())
        if anchor is not None and f12 > 0:
            life = fy_of[(lab, anchor)] / f12
        else:
            life = life_est[(lab, keys[-1])]
        label_life[lab] = life
        fy = sorted((v, x) for (l2, v), x in fy_of.items() if l2 == lab)
        trend = 1.0
        if len(fy) >= 2:
            prev, last = fy[-2][1], fy[-1][1]
            if prev > 0:
                trend = 1.0 + params.lambda_shrink * (float(np.clip(last / prev, lo, hi)) - 1.0)
        disp = _std_or_none(ratios.get(lab, []))
        if disp is None and tier_of[lab] is not None:
            disp = _std_or_none(tier_ratios.get(tier_of[lab], []))
        disp = DEFAULT_LOG_DISPERSION if disp is None else disp
        rows += _profile_rows("label", lab, channels, curve, life, trend, disp, len(keys))
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


BAND_ORDER = ("low", "mid", "high")


def profile_for(profiles: pd.DataFrame, label: str, varietal, price, comparable: Optional[str],
                bands: Dict[str, Tuple[float, float]]) -> Optional[pd.DataFrame]:
    """Fallback chain: the label's own rows, else the comparable label's rows, else tier rows, else
    None. Tier step: tier_key(varietal, price, bands); if that key has no rows, the nearest band of
    the same varietal that has rows (band distance in low < mid < high; ties go to the lower band).
    Magnum labels never fall back to a tier. None also for an unknown varietal or missing price."""
    if profiles is None or profiles.empty:
        return None
    lab = profiles[profiles["scope"] == "label"]
    for name in (label, comparable):
        if isinstance(name, str) and name:
            rows = lab[lab["key"] == name]
            if len(rows):
                return rows
    if is_magnum(str(label)):
        return None
    tk = _safe_tier(varietal, price, bands)
    if tk is None:
        return None
    var, band = tk.rsplit("|", 1)
    i = BAND_ORDER.index(band)
    tiers = profiles[profiles["scope"] == "tier"]
    for j in sorted(range(len(BAND_ORDER)), key=lambda j: (abs(j - i), j)):
        rows = tiers[tiers["key"] == f"{var}|{BAND_ORDER[j]}"]
        if len(rows):
            return rows
    return None


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


DEFAULT_NEXT_MAX_MONTHS = 24   # newest vintage sold fewer months than this -> its successor is a default candidate
MIN_OWN_MONTHS = 3   # months sold before a tier-profiled label's own sales anchor its lifetime


def _own_tier_lifetime(label: str, vintages: pd.DataFrame, curve: np.ndarray, tier_lifetime: float) -> float:
    """Lifetime of a label with sales history that uses a tier profile, anchored on its own sales
    through the tier curve (channels x n, summed over channels):
      - some vintage has a known first_year: first_year(anchor) / tier first-12 share, anchor =
        newest such vintage (if that share > 0);
      - else the newest vintage has months_sold >= 3: sold_to_date(newest) / tier cumulative share
        through min(months_sold, n) (if that share > 0);
      - else the tier lifetime.
    Trend is 1.0 (no two first years to compare), so every vintage of the label gets this value."""
    own = vintages[vintages["label"] == label].sort_values("vintage")
    if own.empty:
        return tier_lifetime
    anchor = _anchor_vintage(vintages, label)
    f12 = float(curve[:, :12].sum())
    if anchor is not None and f12 > 0:
        return float(own.loc[own["vintage"] == anchor, "first_year"].iloc[0]) / f12
    newest = own.iloc[-1]
    ms = int(newest["months_sold"])
    cum = float(curve[:, :min(ms, curve.shape[1])].sum())
    if ms >= MIN_OWN_MONTHS and cum > 0:
        return float(newest["sold_to_date"]) / cum
    return tier_lifetime


def forecast_vintages(profiles: pd.DataFrame, vintages: pd.DataFrame, hist: pd.DataFrame,
                      label_plan: pd.DataFrame, vintage_plan: pd.DataFrame, origin: pd.Timestamp,
                      plan_year: int, params: EngineParams,
                      bands: Dict[str, Tuple[float, float]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Monthly p50/p10/p90 by label-vintage-channel, plus a per-vintage summary.

    Candidates: vintages with months_sold < n, plus each continue/new label's incoming vintage
    (default newest on record + 1 for continue, plan_year - 2 for new), plus, for every label with
    sales whose newest vintage has months_sold < DEFAULT_NEXT_MAX_MONTHS and that has no plan row
    with status 'retire' or naming a different incoming vintage, newest + 1 (unreviewed labels; it
    takes the default release rule and the normal profile chain, magnums included).
    Profile lifetime for vintage V: label-scope profile = lifetime * trend ** (V - anchor), anchor =
    newest vintage of the profile's key label with a known first_year in `vintages` (gap 0 if none;
    negative gaps allowed); tier profile = lifetime (trend 1), except that a label with rows in
    `vintages` anchors on its own sales through the tier curve (_own_tier_lifetime).
    Tier choice in profile_for: exact tier_key, else the nearest band of the varietal (ties to the
    lower band); profile_key reports the tier key actually used.
    Lifetime: existing label = profile lifetime for V; new label (status 'new') with cases known =
    cases x 12 x 0.85, the 0.85 replaced by min(1, comparable profile lifetime / (comparable cases x
    12)) when the profile used is the comparable label's own and vintage_plan has cases > 0 for
    the comparable's anchor vintage; new label with no cases = profile lifetime for V (so with a
    comparable: comparable lifetime * comparable trend ** (V - comparable anchor)). The gap is 0
    when the label-scope profile's curve has first-12 share <= 0 (its lifetime is then not
    first-year anchored). Capped at cases x 12 when cases are known. No profile: skipped with a
    warning, except a non-magnum new label, which gets a summary row only.
    Release: an unreleased candidate whose release month is <= origin's month is moved to the month
    after origin when it has a plan row (warning logged), so no curve mass is dropped; a default
    candidate (no plan row) keeps its month and rolls forward 12 months at a time until after origin.
    Monthly: an already-selling vintage spreads remaining = max(lifetime - sold_to_date, 0) over the
    curve mass from months_sold on (renormalised; remaining = 0 if that mass is 0); a future one
    is lifetime x curve. Rows run from the month after origin to December of plan_year + 1.
    Monthly p10/p90 = p50 x exp(-/+ Z90 x log_dispersion).
    Lifetime: p50 = sold + remaining; p10 = sold + remaining x exp(-Z90 x sigma); p90 = sold +
    remaining x exp(+Z90 x sigma), then capped at max(cases x 12, p50) when cases are known (never
    below sold). For a future vintage sold = 0 and remaining = lifetime."""
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
    n_plan_cands = len(dict.fromkeys(cands))
    for label, g in vintages.groupby("label", sort=True):
        newest = g.loc[g["vintage"].astype(int).idxmax()]
        if int(newest["months_sold"]) >= DEFAULT_NEXT_MAX_MONTHS:
            continue
        nxt = int(newest["vintage"]) + 1
        pr = plan.get(label)
        status = _plan_value(pr, "status")
        if status == "retire":
            continue
        inc = _plan_value(pr, "incoming_vintage")
        if inc is None and status == "new":
            inc = plan_year - 2                      # the plan's own default incoming for a 'new' row
        if status in ("continue", "new") and inc is not None and int(inc) != nxt:
            continue
        cands.append((label, nxt))
    cands = list(dict.fromkeys(cands))
    log.info("bottles: default next-vintage candidates added: %s", len(cands) - n_plan_cands)

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
        if prof is None and (not is_new or is_magnum(str(label))):
            log.warning("bottles: no profile for %s %s (varietal %s); skipped", label, vintage, varietal)
            continue
        if v is not None:
            release = pd.Timestamp(v.release_month)
        else:
            rm = _plan_value(pr, "release_month")
            release = (pd.Timestamp(rm).to_period("M").start_time if rm is not None
                       else _default_release(label, vintage, vintages, plan_year))
            if release <= origin_month and pr is None:
                # default candidate: keep the label's release month, roll forward a year at a time
                rolled = release
                while rolled <= origin_month:
                    rolled = rolled + pd.DateOffset(months=12)
                log.warning("bottles: %s %s default release %s is not after origin; rolled to %s",
                            label, vintage, release.date(), rolled.date())
                release = rolled
            elif release <= origin_month:
                clamped = origin_month + pd.DateOffset(months=1)
                log.warning("bottles: %s %s planned release %s is not after origin; moved to %s",
                            label, vintage, release.date(), clamped.date())
                release = clamped
        c = cases.get((label, vintage), np.nan)
        sold = float(v.sold_to_date) if v is not None else 0.0

        p_life = float(prof["lifetime"].iloc[0]) if prof is not None else np.nan
        p_trend = float(prof["trend"].iloc[0]) if prof is not None else 1.0
        gap = 0
        if prof is not None and prof["scope"].iloc[0] == "label":
            anchor = _anchor_vintage(vintages, prof["key"].iloc[0])
            f12 = float(np.array(list(prof["curve"]), dtype=float)[:, :12].sum())
            gap = vintage - anchor if anchor is not None and f12 > 0 else 0
        disp = float(prof["log_dispersion"].iloc[0]) if prof is not None else DEFAULT_LOG_DISPERSION
        if is_new and pd.notna(c):
            factor = NEW_LABEL_SELL_THROUGH
            if isinstance(comparable, str) and prof is not None and prof["scope"].iloc[0] == "label" \
                    and prof["key"].iloc[0] == comparable:
                ca = _anchor_vintage(vintages, comparable)
                cc = cases.get((comparable, ca), np.nan) if ca is not None else np.nan
                if pd.notna(cc) and cc > 0:
                    factor = min(1.0, p_life / (cc * 12.0))
            life = float(c) * 12.0 * factor
        elif prof is not None and prof["scope"].iloc[0] == "tier":
            life = _own_tier_lifetime(label, vintages, np.array(list(prof["curve"]), dtype=float), p_life)
        else:
            life = p_life * p_trend ** gap
        if pd.notna(c):
            life = min(life, float(c) * 12.0)
        life = 0.0 if not np.isfinite(life) else max(life, 0.0)

        remaining = life - sold if v is not None else life
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
                remaining = remaining if mass > 0 else 0.0
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
        remaining = max(remaining, 0.0)
        life_p50 = sold + remaining
        life_p10 = sold + remaining * np.exp(-Z90 * disp)
        life_p90 = sold + remaining * np.exp(Z90 * disp)
        if pd.notna(c):
            life_p90 = min(life_p90, max(float(c) * 12.0, life_p50))
        summ_rows.append({"label": label, "vintage": vintage, "varietal": varietal, "release_month": release,
                          "sold_to_date": sold, "cases_made": c, "lifetime_p50": life_p50,
                          "lifetime_p10": life_p10, "lifetime_p90": life_p90,
                          "profile_key": (prof["key"].iloc[0] if prof is not None else None)})
    fc = pd.DataFrame(fc_rows, columns=FORECAST_COLUMNS)
    summ = pd.DataFrame(summ_rows, columns=SUMMARY_COLUMNS)
    summ["cases_made"] = summ["cases_made"].astype(float)
    return fc, summ


BACKTEST_COLUMNS = ["label", "vintage", "first_year_actual", "first_year_forecast", "p10", "p90"]


def backtest_vintages(hist: pd.DataFrame, vintages: pd.DataFrame, params: EngineParams) -> pd.DataFrame:
    """Per-vintage first-year retrospective (spec 5.3). One row per label-vintage with a known
    first_year and at least one earlier vintage of the same label with a known first_year:
    forecast = prev.first_year x trend, trend = 1 + lambda_shrink x (clip(prev / prev_prev,
    trend_clip) - 1) from the two vintages just before the target (1.0 with one earlier vintage or
    prev_prev <= 0). Only earlier vintages are used. Magnum labels use their own history.
    sigma = leave-one-out std (ddof=1) of log(actual / forecast) over the other rows with both
    values > 0 (0.35 when fewer than 3); p10/p90 = forecast x exp(-/+ Z90 x sigma). Rows with
    actual or forecast <= 0 are left out of the residual pool and get NaN p10/p90. `hist` is unused
    (kept for symmetry with fit_profiles). Feed the result to fit_profiles(dispersion=...)."""
    if vintages is None or vintages.empty:
        return pd.DataFrame(columns=BACKTEST_COLUMNS)
    lo, hi = params.trend_clip
    rows = []
    known = vintages[vintages["first_year"].notna()]
    for label, g in known.groupby("label", sort=True):
        seq = sorted((int(v), float(x)) for v, x in zip(g["vintage"], g["first_year"]))
        for i in range(1, len(seq)):
            prev = seq[i - 1][1]
            trend = 1.0
            if i >= 2 and seq[i - 2][1] > 0:
                trend = 1.0 + params.lambda_shrink * (float(np.clip(prev / seq[i - 2][1], lo, hi)) - 1.0)
            rows.append({"label": label, "vintage": seq[i][0], "first_year_actual": seq[i][1],
                         "first_year_forecast": prev * trend})
    out = pd.DataFrame(rows, columns=BACKTEST_COLUMNS[:4])
    if out.empty:
        return pd.DataFrame(columns=BACKTEST_COLUMNS)
    a = out["first_year_actual"].to_numpy(float)
    f = out["first_year_forecast"].to_numpy(float)
    ok = (a > 0) & (f > 0)
    resid = np.full(len(out), np.nan)
    resid[ok] = np.log(a[ok] / f[ok])
    p10 = np.full(len(out), np.nan)
    p90 = np.full(len(out), np.nan)
    for i in range(len(out)):
        if not ok[i]:
            continue
        others = np.delete(resid, i)
        others = others[np.isfinite(others)]
        sigma = float(np.std(others, ddof=1)) if len(others) >= MIN_DISPERSION_ROWS else DEFAULT_LOG_DISPERSION
        p10[i] = f[i] * np.exp(-Z90 * sigma)
        p90[i] = f[i] * np.exp(Z90 * sigma)
    out["p10"], out["p90"] = p10, p90
    return out
