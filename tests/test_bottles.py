import logging

import numpy as np
import pandas as pd
import pytest

from forecast import bottles as b
from forecast import io as fio
from forecast.bottles import fit_profiles, forecast_vintages, price_bands, profile_for, vintage_table
from forecast.config import EngineParams
from tests.synth_bottles import CURVE, make_bottle_history, lifetime_bottles

ORIGIN = pd.Timestamp("2026-10-25")
P = EngineParams()


def test_title_parsing():
    assert b.label_from_title("2023 Home Ranch Pinot Noir") == "Home Ranch Pinot Noir"
    assert b.vintage_from_title("2023 Home Ranch Pinot Noir") == 2023
    assert b.varietal_from_title("2023 Home Ranch Pinot Noir") == "Pinot Noir"
    assert b.varietal_from_title("2022 Mikado Tree Pinot Noir - 1.5L") == "Pinot Noir"
    assert b.is_magnum("2022 Mikado Tree Pinot Noir - 1.5L")
    assert not b.is_magnum("2022 Home Ranch Pinot Noir")
    assert b.varietal_from_title("2020 Carneros Chardonnay - 1.5L") == "Chardonnay"
    assert b.varietal_from_title("2021") is None
    assert b.vintage_from_title("Gift Card") is None
    assert b.varietal_from_title("2022 TFV Chardonnay") == "Chardonnay"


def test_vintage_table_release_month_and_first_year():
    vt = b.vintage_table(make_bottle_history(), ORIGIN)
    assert list(vt.columns) == b.VINTAGE_COLUMNS
    r = vt[(vt.label == "Home Ranch Pinot Noir") & (vt.vintage == 2022)].iloc[0]
    assert r.release_month == pd.Timestamp("2024-06-01")
    assert r.months_sold == 29
    assert abs(r.first_year / lifetime_bottles("Home Ranch Pinot Noir", 2022) - 0.48) < 0.03
    young = vt[(vt.label == "Home Ranch Pinot Noir") & (vt.vintage == 2024)].iloc[0]
    assert young.months_sold < 12 and np.isnan(young.first_year)
    assert r.varietal == "Pinot Noir"


def test_tier_key_bands():
    rows = [{"label": l, "varietal": "Pinot Noir", "price": p, "month_start": pd.Timestamp("2025-01-01"),
             "vintage": 2022, "channel": "POS", "bottles": 10, "revenue": 10 * p}
            for l, p in [("A", 60.0), ("B", 90.0), ("C", 130.0)]]
    bands = b.price_bands(pd.DataFrame(rows))
    assert b.tier_key("Pinot Noir", 60.0, bands) == "Pinot Noir|low"
    assert b.tier_key("Pinot Noir", 90.0, bands) == "Pinot Noir|mid"
    assert b.tier_key("Pinot Noir", 130.0, bands) == "Pinot Noir|high"
    two = b.price_bands(pd.DataFrame(rows[:2]))
    assert two["Pinot Noir"] == (75.0, 75.0)


def test_fit_profiles_recovers_planted_curve():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN)
    prof = fit_profiles(h, v, P)
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")]
    total = np.array([sum(c) for c in hr.curve]).sum()
    assert abs(total - 1) < 1e-6
    first12 = sum(sum(c[:12]) for c in hr.curve)
    assert abs(first12 - 0.48) < 0.03
    assert 1.0 < float(hr.trend.iloc[0]) < 1.06


def test_profile_fallback_chain():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    assert profile_for(prof, "Hypersonic Pinot Noir", "Pinot Noir", 120.0, None, bands).scope.iloc[0] in ("label", "tier")
    assert profile_for(prof, "Brand New Pinot Noir", "Pinot Noir", 90.0, "Home Ranch Pinot Noir", bands).key.iloc[0] == "Home Ranch Pinot Noir"
    assert profile_for(prof, "Brand New Pinot Noir", "Pinot Noir", 90.0, None, bands).scope.iloc[0] == "tier"
    assert profile_for(prof, "Mystery Rosé", "Rosé", 40.0, None, bands) is None


def test_forecast_vintages_incoming_and_new_label():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    lp = pd.DataFrame([[2027, "Home Ranch Pinot Noir", "continue", 2025, None, None, None, None, None, None],
                       [2027, "Ridgeline Pinot Noir", "new", 2024, pd.Timestamp("2027-09-01"), None, None, 78.0, "Pinot Noir", None]], columns=fio.EMPTY["label_plan"])
    vp = pd.DataFrame([["Ridgeline Pinot Noir", 2024, 450, False]], columns=fio.EMPTY["vintage_plan"])
    fc, summ = forecast_vintages(prof, v, h, lp, vp, ORIGIN, 2027, P, bands)
    hr25 = fc[(fc.label == "Home Ranch Pinot Noir") & (fc.vintage == 2025)]
    assert hr25.month_start.min() == pd.Timestamp("2027-06-01")
    s = summ[(summ.label == "Ridgeline Pinot Noir")].iloc[0]
    assert s.lifetime_p50 == pytest.approx(450 * 12 * 0.85) and s.lifetime_p10 < s.lifetime_p50 < s.lifetime_p90
    assert (fc.p50 >= 0).all() and fc.p50.notna().all()


def test_forecast_vintages_zero_cases_new_label_is_zero():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    lp = pd.DataFrame([[2027, "Nothing Pinot Noir", "new", 2024, pd.Timestamp("2027-09-01"), None, None, 50.0, "Pinot Noir", None]], columns=fio.EMPTY["label_plan"])
    vp = pd.DataFrame([["Nothing Pinot Noir", 2024, 0, False]], columns=fio.EMPTY["vintage_plan"])
    fc, summ = forecast_vintages(prof, v, h, lp, vp, ORIGIN, 2027, P, bands)
    assert fc[fc.label == "Nothing Pinot Noir"].p50.sum() == 0


def test_already_selling_vintage_respects_sold_to_date_and_cap():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    vp = pd.DataFrame([["Ten Oaks Pinot Noir", 2023, 100, False]], columns=fio.EMPTY["vintage_plan"])   # 1200 cap
    fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), vp, ORIGIN, 2027, P, bands)
    s = summ[(summ.label == "Ten Oaks Pinot Noir") & (summ.vintage == 2023)].iloc[0]
    assert s.lifetime_p50 <= 1200 and fc[(fc.label == "Ten Oaks Pinot Noir") & (fc.vintage == 2023)].p50.sum() <= 1200 - s.sold_to_date + 1e-6


# --- extra checks on the pinned rules -----------------------------------------

def test_profile_columns_and_shape():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P)
    assert list(prof.columns) == b.PROFILE_COLUMNS
    assert all(len(c) == 36 and isinstance(c, list) for c in prof.curve)
    for (_scope, _key), g in prof.groupby(["scope", "key"]):
        assert abs(sum(sum(c) for c in g.curve) - 1) < 1e-9
        assert set(g.channel) == {"Club", "POS", "Web", "Inbound"}
    # Hypersonic has no vintage with >= 24 months sold, so no label profile.
    assert not ((prof.scope == "label") & (prof.key == "Hypersonic Pinot Noir")).any()
    assert ((prof.scope == "tier") & (prof.key == "Pinot Noir|high")).any()
    assert (prof.log_dispersion == 0.35).all()
    assert (prof[prof.scope == "tier"].trend == 1.0).all()


def test_full_lifetime_curve_matches_planted_shape():
    h = make_bottle_history(full_lifetime=True); v = vintage_table(h, pd.Timestamp("2030-01-01"))
    prof = fit_profiles(h, v, P)
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")]
    total_by_month = np.sum(np.array(list(hr.curve)), axis=0)
    assert np.abs(total_by_month - CURVE).max() < 0.005
    # Lifetime = latest qualifying (2024) vintage's 36-month total; trend over 2023 -> 2024 first years.
    life = float(hr.lifetime.iloc[0])
    assert abs(life / lifetime_bottles("Home Ranch Pinot Noir", 2024) - 1) < 0.02
    assert float(hr.trend.iloc[0]) == pytest.approx(1 + 0.7 * 0.05, abs=0.01)


def test_lifetime_extrapolated_for_partial_latest_vintage():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P)
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")]
    # Latest qualifying vintage is 2022 (29 months sold); extrapolation lands near its true lifetime.
    assert abs(float(hr.lifetime.iloc[0]) / lifetime_bottles("Home Ranch Pinot Noir", 2022) - 1) < 0.06


def test_log_dispersion_from_dispersion_frame():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN)
    disp = pd.DataFrame({"label": ["Ten Oaks Pinot Noir"] * 3, "vintage": [2021, 2022, 2023],
                         "first_year_actual": [100.0, 120.0, 90.0], "first_year_forecast": [100.0, 100.0, 100.0]})
    prof = fit_profiles(h, v, P, dispersion=disp)
    expect = float(np.std(np.log([1.0, 1.2, 0.9]), ddof=1))
    lab = prof[prof.scope == "label"].groupby("key").log_dispersion.first()
    assert lab["Ten Oaks Pinot Noir"] == pytest.approx(expect)
    # Three Hills ($58) has no rows of its own but shares Ten Oaks' low tier, which has 3 rows.
    assert lab["Three Hills Pinot Noir"] == pytest.approx(expect)
    assert float(prof[(prof.scope == "tier") & (prof.key == "Pinot Noir|low")].log_dispersion.iloc[0]) == pytest.approx(expect)
    # Home Ranch ($62) is in the mid tier with no rows: default.
    assert lab["Home Ranch Pinot Noir"] == 0.35


def test_forecast_vintages_columns_window_and_bands():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    assert list(fc.columns) == ["label", "vintage", "month_start", "channel", "p50", "p10", "p90"]
    assert list(summ.columns) == ["label", "vintage", "varietal", "release_month", "sold_to_date", "cases_made",
                                  "lifetime_p50", "lifetime_p10", "lifetime_p90", "profile_key"]
    assert fc.month_start.min() == pd.Timestamp("2026-11-01") and fc.month_start.max() <= pd.Timestamp("2028-12-01")
    assert summ.cases_made.isna().all()
    k = np.exp(1.2816 * 0.35)
    assert np.allclose(fc.p90, fc.p50 * k) and np.allclose(fc.p10, fc.p50 / k)
    # Hypersonic (no label profile) falls back to its tier.
    hy = summ[summ.label == "Hypersonic Pinot Noir"]
    assert set(hy.profile_key) == {"Pinot Noir|high"}
    # Every candidate with months_sold < 36 has a summary row; Home Ranch 2021 (41 months) does not.
    assert not ((summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2021)).any()
    assert ((summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2024)).any()
    # Remaining for an already-selling vintage never goes below zero and starts after origin.
    hr24 = fc[(fc.label == "Home Ranch Pinot Noir") & (fc.vintage == 2024)]
    assert hr24.month_start.min() == pd.Timestamp("2026-11-01") and (hr24.p50 >= 0).all()


def test_future_vintage_after_window_has_summary_only_and_comparable_scaling():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    lp = pd.DataFrame([[2027, "Late Pinot Noir", "new", 2025, pd.Timestamp("2029-03-01"), None, None, 60.0, "Pinot Noir", None],
                       [2027, "Copy Pinot Noir", "new", 2025, pd.Timestamp("2027-09-01"), None, None, 60.0, "Pinot Noir",
                        "Home Ranch Pinot Noir"]], columns=fio.EMPTY["label_plan"])
    # Home Ranch's latest qualifying vintage is 2022; give it a generous cases figure so ratio < 1.
    vp = pd.DataFrame([["Late Pinot Noir", 2025, 300, False], ["Copy Pinot Noir", 2025, 300, False],
                       ["Home Ranch Pinot Noir", 2022, 1000, False]], columns=fio.EMPTY["vintage_plan"])
    fc, summ = forecast_vintages(prof, v, h, lp, vp, ORIGIN, 2027, P, bands)
    assert fc[fc.label == "Late Pinot Noir"].empty
    late = summ[summ.label == "Late Pinot Noir"].iloc[0]
    assert late.lifetime_p50 == pytest.approx(300 * 12 * 0.85) and late.release_month == pd.Timestamp("2029-03-01")
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")]
    ratio = min(1.0, float(hr.lifetime.iloc[0]) / (1000 * 12))
    cp = summ[summ.label == "Copy Pinot Noir"].iloc[0]
    assert cp.lifetime_p50 == pytest.approx(300 * 12 * ratio) and cp.profile_key == "Home Ranch Pinot Noir"


def test_existing_label_incoming_without_profile_is_skipped(caplog):
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    prof = prof[prof.key != "Pinot Noir|high"]   # Hypersonic now has no profile at all
    lp = pd.DataFrame([[2027, "Hypersonic Pinot Noir", "continue", None, None, None, None, None, None, None]],
                      columns=fio.EMPTY["label_plan"])
    with caplog.at_level(logging.WARNING, logger="forecast"):
        fc, summ = forecast_vintages(prof, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    assert not ((summ.label == "Hypersonic Pinot Noir") & (summ.vintage == 2025)).any()
    assert "Hypersonic Pinot Noir" in caplog.text
