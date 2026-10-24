"""Per-KPI daily forecast orchestration (spec §4.2–4.8)."""
from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd

from .config import (ACTIVE_MEMBERS_KPI, ATTRITION_KPI, CLUB_KPIS, EVENT_CALENDAR_FIELD,
                     EVENT_KPIS, NEW_MEMBERS_KPI, OTB_KPIS, RATE_KPIS, SEASONAL_KPIS)
from .identities import apply_identities
from .models.club import release_component, release_magnitude, residual_series
from .models.events import cap_series, event_component
from .models.membership import (attrition_base_series, project_stock, spike_component,
                                spike_profile)
from .models.otb import otb_series
from .models.seasonal import apply_floor, swm_forecast
from .types import Context, ScheduledRelease


def forecast_daily(hist: pd.DataFrame, origin: pd.Timestamp, dates: pd.DatetimeIndex,
                   ctx: Context) -> Tuple[pd.DataFrame, Dict[int, pd.DataFrame]]:
    p = ctx.params
    dates = pd.DatetimeIndex(dates)
    h = hist.loc[:origin]
    if ACTIVE_MEMBERS_KPI in h:
        members_hist = h[ACTIVE_MEMBERS_KPI].astype(float)
    else:
        members_hist = pd.Series(float(ctx.members_a0), index=h.index)
    point = pd.DataFrame(index=dates)
    comps: Dict[int, pd.DataFrame] = {}

    # --- Seasonal flows and the directly-forecast rate KPI -------------------
    for k in SEASONAL_KPIS + RATE_KPIS:
        fc = swm_forecast(h[k], origin, dates, p)
        if k not in RATE_KPIS:
            fc = apply_floor(fc, k, h[k])
        point[k] = fc
        comps[k] = pd.DataFrame({"organic": fc})

    # --- Event-sensitive flows --------------------------------------------
    for k in EVENT_KPIS:
        fc = apply_floor(swm_forecast(cap_series(h[k], p.event_cap_pct), origin, dates, p), k, h[k])
        if k in EVENT_CALENDAR_FIELD:
            ev = event_component(ctx.events, EVENT_CALENDAR_FIELD[k], dates)
        else:
            ev = pd.Series(0.0, index=dates)
        point[k] = fc + ev
        comps[k] = pd.DataFrame({"organic": fc, "event": ev})

    # --- Membership ----------------------------------------------------------
    new = apply_floor(swm_forecast(h[NEW_MEMBERS_KPI], origin, dates, p), NEW_MEMBERS_KPI, h[NEW_MEMBERS_KPI])
    base_series = attrition_base_series(h[ATTRITION_KPI], ctx.releases, p.attrition_spike_days)
    base_attr = apply_floor(swm_forecast(base_series, origin, dates, p), ATTRITION_KPI, base_series)
    profile = spike_profile(h[ATTRITION_KPI], ctx.releases, members_hist,
                            p.attrition_spike_days, p.release_lookback_n)
    if ctx.members_a0:
        a0 = float(ctx.members_a0)
    else:
        a0 = float(members_hist.iloc[-1]) if len(members_hist) else 0.0
    recent = [ScheduledRelease(start=r.start, days=r.days, label="recent")
              for r in ctx.releases
              if r.start > origin - pd.Timedelta(days=p.attrition_spike_days)]
    spike = pd.Series(0.0, index=dates)
    attr = base_attr.copy()
    for s in sorted(recent + list(ctx.schedule), key=lambda s: s.start):
        if s.start > dates[-1]:
            break
        if s.start <= origin:
            m = a0
        else:
            m = a0 + float((new - attr).loc[: s.start - pd.Timedelta(days=1)].sum())
        spike = spike + spike_component([s], profile, lambda _d, m=m: m, dates)
        attr = base_attr + spike
    stock = project_stock(a0, new, attr)
    point[NEW_MEMBERS_KPI] = new
    point[ATTRITION_KPI] = attr
    point[ACTIVE_MEMBERS_KPI] = stock
    comps[NEW_MEMBERS_KPI] = pd.DataFrame({"organic": new})
    comps[ATTRITION_KPI] = pd.DataFrame({"organic": base_attr, "spike": spike})
    comps[ACTIVE_MEMBERS_KPI] = pd.DataFrame({"organic": stock})

    # --- Club-sensitive flows ----------------------------------------------
    for k in CLUB_KPIS:
        resid = residual_series(h[k], ctx.releases)
        fc = apply_floor(swm_forecast(resid, origin, dates, p), k, resid)
        rpm, shares = release_magnitude(h[k], ctx.releases, members_hist, p.release_lookback_n)
        rel = release_component(ctx.schedule, rpm, shares, stock, dates)
        point[k] = fc + rel
        comps[k] = pd.DataFrame({"organic": fc, "release": rel})

    # --- On-the-books floors -------------------------------------------------
    within = dates <= origin + pd.Timedelta(days=p.otb_days)
    for k, (field, attribution) in OTB_KPIS.items():
        otb = otb_series(ctx.otb, field, attribution, dates)
        floor = otb.where(within)
        current = point[k].to_numpy(dtype=float)
        raised = floor.notna().to_numpy() & (floor.to_numpy(dtype=float) > current)
        point[k] = pd.Series(np.where(raised, floor.to_numpy(dtype=float), current), index=dates)
        comps[k]["otb"] = otb

    point = apply_identities(point)
    return point, comps
