"""Per-KPI champion selection on the rolling month-ahead backtest (spec 5.2)."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config import (CLUB_KPIS, HEADLINE_GUARD_KPIS, RATIO_IDENTITIES, SELECTABLE_KPIS,
                     SUM_IDENTITIES, EngineParams)
from .types import Champion

_POINT_COLUMNS = ["kpi_id", "period_start", "actual", "forecast"]
_EPS = 1e-12


def _raw_month_ahead(pf: pd.DataFrame) -> pd.DataFrame:
    if pf is None or pf.empty:
        return pd.DataFrame(columns=_POINT_COLUMNS)
    m = pf[(pf["period"] == "month") & (pf["ahead"] == 1)]
    return m[_POINT_COLUMNS].reset_index(drop=True)


def month_ahead_points(pf: pd.DataFrame) -> pd.DataFrame:
    """Month-ahead=1 period rows, with months where actual and forecast are both zero dropped."""
    if pf is None or pf.empty:
        return pd.DataFrame(columns=_POINT_COLUMNS)
    m = pf[(pf["period"] == "month") & (pf["ahead"] == 1)]
    m = m[~((m["actual"] == 0) & (m["forecast"] == 0))]
    return m[_POINT_COLUMNS].reset_index(drop=True)


def loo_calibration(a: np.ndarray, f: np.ndarray, shrink: float) -> np.ndarray:
    """Leave-one-out multipliers m_i = 1 / (1 + shrink * (c_i - 1)), c_i = sum_{j!=i} f / sum_{j!=i} a."""
    a = np.asarray(a, dtype=float)
    f = np.asarray(f, dtype=float)
    out = np.ones(len(a))
    for i in range(len(a)):
        sa = a.sum() - a[i]
        if sa == 0:
            continue
        c = (f.sum() - f[i]) / sa
        out[i] = 1.0 / (1.0 + shrink * (c - 1.0))
    return out


def _score(a: np.ndarray, f: np.ndarray) -> float:
    return float(np.abs(a - f).sum() / np.abs(a).sum())


def grid_scores(a, f_swm, f_level, params: EngineParams) -> List[dict]:
    """wMAPE of every (weight, calibration) cell; empty when the actuals sum to zero."""
    a = np.asarray(a, dtype=float)
    f_swm = np.asarray(f_swm, dtype=float)
    f_level = np.asarray(f_level, dtype=float)
    if len(a) == 0 or np.abs(a).sum() == 0:
        return []
    out: List[dict] = []
    for w in params.champion_weights:
        f = w * f_swm + (1.0 - w) * f_level
        m = loo_calibration(a, f, params.calibration_shrink)
        out.append({"w": float(w), "cal": False, "wmape": _score(a, f)})
        out.append({"w": float(w), "cal": True, "wmape": _score(a, f * m)})
    return out


def _live_c(a: np.ndarray, f: np.ndarray):
    sa = a.sum()
    return None if sa == 0 else float(f.sum() / sa)


def select_champions(pf_swm: pd.DataFrame, pf_level: pd.DataFrame,
                     params: EngineParams) -> Dict[int, Champion]:
    """Pick each selectable KPI's champion from the two month-ahead=1 frames.

    The frames are joined on period_start first; only months where the actual and both
    forecasts are zero are then dropped, as are rows with a non-finite actual or forecast.
    The default (w=1, uncalibrated) is replaced only by a challenger at least champion_margin
    better, with at least champion_min_months months. Ties among challengers go to the cell
    nearest the default: higher w first, then uncalibrated before calibrated. Calibrated
    cells are scored but never adopted for club KPIs (the live chain calibrates their
    organic residual only). Finally the headline guard reverts champions that make a
    derived headline KPI worse (headline_guard)."""
    ps, pl = _raw_month_ahead(pf_swm), _raw_month_ahead(pf_level)
    out: Dict[int, Champion] = {}
    for k in SELECTABLE_KPIS:
        s, l = ps[ps["kpi_id"] == k], pl[pl["kpi_id"] == k]
        if s.empty or l.empty:
            continue
        j = s.merge(l, on="period_start", suffixes=("_swm", "_level")).sort_values("period_start")
        j = j[np.isfinite(j[["actual_swm", "forecast_swm", "forecast_level"]].astype(float)).all(axis=1)]
        j = j[~((j["actual_swm"] == 0) & (j["forecast_swm"] == 0) & (j["forecast_level"] == 0))]
        a = j["actual_swm"].to_numpy(dtype=float)
        fs = j["forecast_swm"].to_numpy(dtype=float)
        fl = j["forecast_level"].to_numpy(dtype=float)
        n = int(len(j))
        grid = grid_scores(a, fs, fl, params)
        champ = Champion.default()
        champ.n, champ.candidates = n, grid
        default = next((g for g in grid if g["w"] == 1.0 and not g["cal"]), None)
        if default is not None:
            champ.wmape_default = champ.wmape_selection = default["wmape"]
        chosen = None
        if default is not None and n >= params.champion_min_months:
            best = None
            for g in sorted(grid, key=lambda g: (-g["w"], g["cal"])):
                if g["w"] == 1.0 and not g["cal"]:
                    continue
                if g["cal"]:
                    if k in CLUB_KPIS:
                        continue
                    c = _live_c(a, g["w"] * fs + (1.0 - g["w"]) * fl)
                    if c is None or abs(c - 1.0) > params.calibration_max:
                        continue
                if best is None or g["wmape"] < best["wmape"]:
                    best = g
            if best is not None and default["wmape"] - best["wmape"] >= params.champion_margin - _EPS:
                chosen = best
        if chosen is not None:
            w = chosen["w"]
            champ.w_swm, champ.model = w, Champion.name_for(w)
            champ.wmape_selection = chosen["wmape"]
            if chosen["cal"]:
                c = _live_c(a, w * fs + (1.0 - w) * fl)
                champ.calibration = 1.0 / (1.0 + params.calibration_shrink * (c - 1.0))
        out[k] = champ
    return headline_guard(out, pf_swm, pf_level, params)


def _wide(points: pd.DataFrame, value: str) -> pd.DataFrame:
    if points.empty:
        return pd.DataFrame()
    return points.pivot_table(index="period_start", columns="kpi_id", values=value, aggfunc="last")


def _guard_parts(g: int) -> Tuple[List[int], Optional[List[int]], float]:
    """(numerator parts, denominator parts or None for a sum, scale) of a guarded KPI."""
    if g in SUM_IDENTITIES:
        return list(SUM_IDENTITIES[g]), None, 1.0
    if g in RATIO_IDENTITIES:
        nums, dens, scale = RATIO_IDENTITIES[g]
        return list(nums), list(dens), float(scale)
    return [g], None, 1.0


class _GuardFrames:
    """Aligned month-ahead=1 period values: actuals and swm forecasts of every KPI from the
    swm frame, level forecasts of the selectable KPIs from the level frame."""

    def __init__(self, pf_swm: pd.DataFrame, pf_level: pd.DataFrame):
        ps, pl = _raw_month_ahead(pf_swm), _raw_month_ahead(pf_level)
        actual, f_swm, f_level = _wide(ps, "actual"), _wide(ps, "forecast"), _wide(pl, "forecast")
        # One shared index: a month whose value is NaN for every KPI in one pivot would
        # otherwise be missing from that frame alone and break the element-wise maths.
        index = actual.index.union(f_swm.index).union(f_level.index)
        self.actual = actual.reindex(index)
        self.f_swm = f_swm.reindex(index)
        self.f_level = f_level.reindex(index)

    def forecast(self, k: int, champions: Dict[int, Champion]) -> np.ndarray:
        """k's period forecasts under `champions`: m * (w*swm + (1-w)*level) for a selectable
        KPI with a champion; the swm frame's values otherwise (identical in both frames)."""
        if k not in self.f_swm:
            return np.zeros(len(self.actual))
        fs = self.f_swm[k].to_numpy(dtype=float)
        c = champions.get(k) if k in SELECTABLE_KPIS else None
        if c is None or c.is_default:
            return fs
        w = float(c.w_swm)
        if w >= 1.0:
            base = fs
        else:
            fl = (self.f_level[k] if k in self.f_level else pd.Series(np.nan, index=self.actual.index))
            base = w * fs + (1.0 - w) * fl.to_numpy(dtype=float)
        return base * float(c.calibration)

    def _actual(self, k: int) -> np.ndarray:
        if k not in self.actual:
            return np.zeros(len(self.actual))
        return self.actual[k].to_numpy(dtype=float)

    def _value(self, g: int, get) -> np.ndarray:
        nums, dens, scale = _guard_parts(g)
        n = sum((get(p) for p in nums), np.zeros(len(self.actual)))
        if dens is None:
            return n
        d = sum((get(p) for p in dens), np.zeros(len(self.actual)))
        out = np.zeros(len(self.actual))
        ok = d != 0
        out[ok] = n[ok] / d[ok] * scale
        return np.where(np.isfinite(n) & np.isfinite(d), out, np.nan)

    def wmapes(self, g: int, a_champs: Dict[int, Champion],
               b_champs: Dict[int, Champion]) -> Tuple[Optional[float], Optional[float]]:
        """Month-ahead wMAPE of guarded KPI g under two champion maps, on the same months:
        finite everywhere, and not zero in the actual and both forecasts."""
        if self.actual.empty:
            return None, None
        a = self._value(g, self._actual)
        fa = self._value(g, lambda k: self.forecast(k, a_champs))
        fb = self._value(g, lambda k: self.forecast(k, b_champs))
        keep = np.isfinite(a) & np.isfinite(fa) & np.isfinite(fb) & ~((a == 0) & (fa == 0) & (fb == 0))
        if not keep.any() or np.abs(a[keep]).sum() == 0:
            return None, None
        return _score(a[keep], fa[keep]), _score(a[keep], fb[keep])


def _reverted(c: Champion, g: int) -> Champion:
    r = Champion.default()
    r.n, r.candidates, r.wmape_default, r.reverted_for = c.n, c.candidates, c.wmape_default, g
    r.wmape_selection = c.wmape_default
    return r


def headline_guard(champions: Dict[int, Champion], pf_swm: pd.DataFrame, pf_level: pd.DataFrame,
                   params: EngineParams) -> Dict[int, Champion]:
    """Revert champions that make a headline derived KPI worse (spec 5.3, ruling 2026-10-27).

    Each KPI in HEADLINE_GUARD_KPIS is scored on its month-ahead=1 period values rebuilt from
    its parts (a sum, or sum of numerators over sum of denominators times scale per period)
    under the chosen champions and under the defaults. While one is worse than the default
    by more than champion_margin, the selectable part with a non-default champion whose
    reversion improves that KPI most is reverted, and everything is re-scored."""
    out = dict(champions)
    frames = _GuardFrames(pf_swm, pf_level)
    defaults: Dict[int, Champion] = {}
    while True:
        reverted = False
        for g in HEADLINE_GUARD_KPIS:
            chosen, dflt = frames.wmapes(g, out, defaults)
            if chosen is None or dflt is None or chosen - dflt <= params.champion_margin + _EPS:
                continue
            nums, dens, _ = _guard_parts(g)
            parts = [p for p in dict.fromkeys(nums + (dens or []))
                     if p in SELECTABLE_KPIS and p in out and not out[p].is_default]
            best, best_score = None, None
            for p in parts:
                trial = dict(out)
                trial[p] = Champion.default()
                score, _ = frames.wmapes(g, trial, defaults)
                if score is not None and (best_score is None or score < best_score):
                    best, best_score = p, score
            if best is None:
                continue
            out[best] = _reverted(out[best], g)
            reverted = True
            break
        if not reverted:
            return out
