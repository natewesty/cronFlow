"""Per-KPI champion selection on the rolling month-ahead backtest (spec 5.2)."""
from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from .config import SELECTABLE_KPIS, EngineParams
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
    nearest the default: higher w first, then uncalibrated before calibrated."""
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
    return out
