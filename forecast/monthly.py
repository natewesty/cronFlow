"""Monthly component forecast for the Plan Builder (spec §4.9)."""
from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from .config import (ACTIVE_MEMBERS_KPI, ALL_KPIS, ATTRITION_KPI, CLUB_KPIS, DERIVED_KPIS,
                     EVENT_CALENDAR_FIELD, EVENT_KPIS, MODELED_KPIS, NEW_MEMBERS_KPI,
                     RATE_KPIS)
from .identities import apply_identities
from .models.club import release_magnitude, residual_series
from .models.events import cap_series
from .models.membership import attrition_base_series, spike_profile
from .types import Context

MONTHLY_COLUMNS = ["month_start", "kpi_id", "is_actual", "organic", "organic_neutral",
                   "release", "event", "fitted_growth", "p50"]


def _resample(s: pd.Series, k: int) -> pd.Series:
    if k in RATE_KPIS:
        return s.resample("MS").mean()
    if k == ACTIVE_MEMBERS_KPI:
        return s.resample("MS").last()
    return s.resample("MS").sum()


def _growth(mh: pd.Series, params) -> float:
    if len(mh) < 24:
        return 1.0
    last12 = float(mh.iloc[-12:].sum())
    prev12 = float(mh.iloc[-24:-12].sum())
    if prev12 <= 0 or last12 <= 0:
        return 1.0
    lo, hi = params.trend_clip
    g = float(np.clip(last12 / prev12, lo, hi))
    return 1.0 + params.lambda_shrink * (g - 1.0)


def _base(mh: pd.Series, month: pd.Timestamp, w1: float, w2: float) -> float:
    b1 = mh.get(month - pd.DateOffset(years=1), np.nan)
    b2 = mh.get(month - pd.DateOffset(years=2), np.nan)
    if np.isfinite(b1) and np.isfinite(b2):
        return float(w1 * b1 + w2 * b2)
    if np.isfinite(b1):
        return float(b1)
    if np.isfinite(b2):
        return float(b2)
    return float(mh.iloc[-3:].mean()) if len(mh) else 0.0


def monthly_forecast(hist: pd.DataFrame, origin: pd.Timestamp, ctx: Context,
                     daily_point: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    p = ctx.params
    h = hist.loc[:origin]
    cur_month = origin.normalize().replace(day=1)
    months = pd.date_range(cur_month - pd.DateOffset(months=p.monthly_lookback_months),
                           periods=p.monthly_lookback_months + p.monthly_months, freq="MS")
    members_hist = h[ACTIVE_MEMBERS_KPI].astype(float)
    last_complete = cur_month - pd.Timedelta(days=1)

    # Cleaned series per modeled KPI, then complete-month history of each.
    cleaned: Dict[int, pd.Series] = {}
    for k in MODELED_KPIS:
        if k in CLUB_KPIS:
            cleaned[k] = residual_series(h[k], ctx.releases)
        elif k in EVENT_KPIS:
            cleaned[k] = cap_series(h[k], p.event_cap_pct)
        elif k == ATTRITION_KPI:
            cleaned[k] = attrition_base_series(h[k], ctx.releases, p.attrition_spike_days)
        else:
            cleaned[k] = h[k].astype(float)
    mh = {k: _resample(cleaned[k].loc[:last_complete], k) for k in MODELED_KPIS}
    raw_mh = {k: _resample(h[k].astype(float).loc[:last_complete], k) for k in MODELED_KPIS}
    growth = {k: _growth(mh[k], p) for k in MODELED_KPIS}

    # Release magnitudes per club KPI and the attrition spike mass per release.
    magnitudes = {k: release_magnitude(h[k], ctx.releases, members_hist, p.release_lookback_n)[0]
                  for k in CLUB_KPIS}
    spike_mass = float(spike_profile(h[ATTRITION_KPI], ctx.releases, members_hist,
                                     p.attrition_spike_days, p.release_lookback_n).sum())

    def releases_in(month: pd.Timestamp):
        end = month + pd.DateOffset(months=1)
        return [s for s in ctx.schedule if month <= s.start < end and s.start > origin]

    def hist_release_total(k: int, month: pd.Timestamp) -> float:
        end = month + pd.DateOffset(months=1)
        tot = 0.0
        for r in ctx.releases:
            if month <= r.start < end:
                tot += float(h[k].reindex(r.dates).fillna(0.0).sum())
        return tot

    def events_in(month: pd.Timestamp, field: str) -> float:
        end = month + pd.DateOffset(months=1)
        tot = 0.0
        for e in ctx.events or []:
            try:
                d = pd.Timestamp(e.get("date"))
            except (ValueError, TypeError):
                continue
            if pd.notna(d) and month <= d < end:
                tot += float(e.get(field) or 0.0)
        return tot

    rows = []
    stock = float(ctx.members_a0) if ctx.members_a0 else float(members_hist.iloc[-1])
    month_end_cur = cur_month + pd.DateOffset(months=1) - pd.Timedelta(days=1)
    days_in_cur = (month_end_cur - cur_month).days + 1
    elapsed = (origin - cur_month).days + 1
    remaining_frac = max(0.0, (days_in_cur - elapsed) / days_in_cur)

    for month in months:
        is_actual = month < cur_month
        is_current = month == cur_month
        p50_by_kpi: Dict[int, float] = {}
        month_rows = []
        # Membership first (stock feeds club releases).
        if is_actual:
            new_m = float(raw_mh[NEW_MEMBERS_KPI].get(month, 0.0))
            attr_m = float(raw_mh[ATTRITION_KPI].get(month, 0.0))
            stock_m = float(raw_mh[ACTIVE_MEMBERS_KPI].get(month, stock))
        else:
            new_org_n = _base(mh[NEW_MEMBERS_KPI], month, p.w1, p.w2)
            attr_org_n = _base(mh[ATTRITION_KPI], month, p.w1, p.w2)
            new_m = new_org_n * growth[NEW_MEMBERS_KPI]
            attr_base = attr_org_n * growth[ATTRITION_KPI]
            spike_m = sum(spike_mass * stock for _ in releases_in(month))
            if is_current:
                new_m = float(h.loc[cur_month:origin, NEW_MEMBERS_KPI].sum()) + new_m * remaining_frac
                attr_base = float(h.loc[cur_month:origin, ATTRITION_KPI].sum()) + attr_base * remaining_frac
            attr_m = attr_base + spike_m
            stock_m = stock + new_m - attr_m
        for k in MODELED_KPIS:
            organic = organic_n = release = event = np.nan
            if is_actual:
                p50 = float(raw_mh[k].get(month, 0.0))
                organic = organic_n = p50
                release = hist_release_total(k, month) if k in CLUB_KPIS else 0.0
                event = 0.0
            elif k == NEW_MEMBERS_KPI:
                organic = new_m
                organic_n = new_org_n
                release = event = 0.0
                p50 = new_m
            elif k == ATTRITION_KPI:
                organic = attr_base
                organic_n = attr_org_n
                release = spike_m
                event = 0.0
                p50 = attr_m
            elif k == ACTIVE_MEMBERS_KPI:
                organic = organic_n = p50 = stock_m
                release = event = 0.0
            else:
                organic_n = _base(mh[k], month, p.w1, p.w2)
                organic = organic_n * growth[k]
                release = sum(magnitudes[k] * stock for _ in releases_in(month)) if k in CLUB_KPIS else 0.0
                event = events_in(month, EVENT_CALENDAR_FIELD[k]) if k in EVENT_CALENDAR_FIELD else 0.0
                if is_current:
                    # Releases/events scheduled after origin inside this month count in full;
                    # only the organic part is prorated by the remaining fraction.
                    mtd = h.loc[cur_month:origin, k]
                    if daily_point is not None and k in daily_point:
                        rest = daily_point.loc[origin + pd.Timedelta(days=1):month_end_cur, k]
                        if k in RATE_KPIS:
                            p50 = float(pd.concat([mtd, rest]).mean())
                        else:
                            p50 = float(mtd.sum() + rest.sum())
                    elif k in RATE_KPIS:
                        p50 = float(mtd.mean()) if len(mtd) else organic
                    else:
                        p50 = float(mtd.sum()) + organic * remaining_frac + release + event
                    organic = organic_n = p50 if k in RATE_KPIS else p50 - release - event
                else:
                    p50 = organic + release + event
            if is_current and k != ACTIVE_MEMBERS_KPI:
                # Current-month organic already carries fitted growth; strip it so the
                # app does not apply growth to MTD actuals a second time.
                organic_n = organic / growth[k]
            p50_by_kpi[k] = p50
            month_rows.append({"month_start": month, "kpi_id": k, "is_actual": is_actual,
                               "organic": organic, "organic_neutral": organic_n, "release": release,
                               "event": event,
                               "fitted_growth": 1.0 if k == ACTIVE_MEMBERS_KPI else growth[k],
                               "p50": p50})
        stock = stock_m
        # Derived KPIs from identities on this month's p50 values.
        wide = pd.DataFrame([p50_by_kpi], index=[month])
        derived = apply_identities(wide).iloc[0]
        for k in DERIVED_KPIS:
            month_rows.append({"month_start": month, "kpi_id": k, "is_actual": is_actual,
                               "organic": np.nan, "organic_neutral": np.nan, "release": np.nan,
                               "event": np.nan, "fitted_growth": np.nan, "p50": float(derived[k])})
        rows.extend(month_rows)

    out = pd.DataFrame(rows, columns=MONTHLY_COLUMNS)
    out = out.sort_values(["month_start", "kpi_id"]).reset_index(drop=True)
    return out
