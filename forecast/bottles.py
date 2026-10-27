"""Bottle label parsing, per-vintage table and price tiers (spec 5.1, 5.3). Pure pandas, no IO."""
from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

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
