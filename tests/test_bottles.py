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
    hr25s = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2025)].iloc[0]
    assert abs(hr25s.lifetime_p50 / (4000 * 1.05 ** 4) - 1) < 0.10
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
    # Magnums keep a label profile but are in no tier; without Mikado, Pinot Noir|high is empty.
    assert ((prof.scope == "label") & (prof.key == "Mikado Tree Pinot Noir - 1.5L")).any()
    assert set(prof[prof.scope == "tier"].key) == {"Pinot Noir|low", "Pinot Noir|mid", "Chardonnay|low"}
    assert (prof.log_dispersion == 0.35).all()
    assert (prof[prof.scope == "tier"].trend == 1.0).all()


def test_full_lifetime_curve_matches_planted_shape():
    h = make_bottle_history(full_lifetime=True); v = vintage_table(h, pd.Timestamp("2030-01-01"))
    prof = fit_profiles(h, v, P)
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")]
    total_by_month = np.sum(np.array(list(hr.curve)), axis=0)
    assert np.abs(total_by_month - CURVE).max() < 0.005
    # Lifetime = first_year(2024 anchor) / first-12 share; trend over 2023 -> 2024 first years.
    life = float(hr.lifetime.iloc[0])
    assert abs(life / lifetime_bottles("Home Ranch Pinot Noir", 2024) - 1) < 0.02
    assert float(hr.trend.iloc[0]) == pytest.approx(1 + 0.7 * 0.05, abs=0.01)


def test_incomplete_vintage_completed_from_tier_and_anchor_lifetime():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P)
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")]
    curve = np.sum(np.array(list(hr.curve)), axis=0)
    # 2022 (29 months) is completed with the tier's complete-vintage tail, so the tail is not lost.
    assert abs(curve[29:].sum() - CURVE[29:].sum()) < 0.01
    assert abs(curve[:12].sum() - 0.48) < 0.01
    # Lifetime anchors on the newest vintage with a first year (2023, 17 months sold).
    assert abs(float(hr.lifetime.iloc[0]) / lifetime_bottles("Home Ranch Pinot Noir", 2023) - 1) < 0.03


def test_complete_curve_rules():
    ref = np.array([[0.25, 0.25, 0.25, 0.25]])
    slots = np.array([[30.0, 10.0, 0.0, 0.0]])
    out, est = b._complete_curve(slots, 2, ref)
    assert est == pytest.approx(80.0)
    assert np.allclose(out, [[30 / 80, 10 / 80, 0.25, 0.25]]) and out.sum() == pytest.approx(1.0)
    out, est = b._complete_curve(slots, 2, None)
    assert est == 40.0 and np.allclose(out, [[0.75, 0.25, 0.0, 0.0]])


def test_magnums_out_of_bands_and_skipped_without_label_profile(caplog):
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    assert bands["Pinot Noir"] == (58.0, 62.0)   # Mikado ($160) excluded
    assert profile_for(prof, "Mikado Tree Pinot Noir - 1.5L", "Pinot Noir", 160.0, None, bands).scope.iloc[0] == "label"
    prof2 = prof[prof.key != "Mikado Tree Pinot Noir - 1.5L"]
    assert profile_for(prof2, "Mikado Tree Pinot Noir - 1.5L", "Pinot Noir", 160.0, None, bands) is None
    lp = pd.DataFrame([[2027, "Big Pinot Noir - 1.5L", "new", 2025, pd.Timestamp("2027-09-01"), None, None, 150.0,
                        "Pinot Noir", None]], columns=fio.EMPTY["label_plan"])
    with caplog.at_level(logging.WARNING, logger="forecast"):
        fc, summ = forecast_vintages(prof2, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    assert not summ.label.isin(["Mikado Tree Pinot Noir - 1.5L", "Big Pinot Noir - 1.5L"]).any()
    assert "Mikado Tree Pinot Noir - 1.5L" in caplog.text and "Big Pinot Noir - 1.5L" in caplog.text


def test_tier_nearest_band_fallback():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    # High band has no qualifying label: nearest is mid.
    assert profile_for(prof, "X Pinot Noir", "Pinot Noir", 120.0, None, bands).key.iloc[0] == "Pinot Noir|mid"
    no_mid = prof[prof.key != "Pinot Noir|mid"]
    assert profile_for(no_mid, "X Pinot Noir", "Pinot Noir", 60.0, None, bands).key.iloc[0] == "Pinot Noir|low"


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
    # Hypersonic (no label profile, high band empty) falls back to the nearest band.
    hy = summ[summ.label == "Hypersonic Pinot Noir"]
    assert set(hy.profile_key) == {"Pinot Noir|mid"}
    # ...but its lifetime anchors on its own 2023 first year, not on Home Ranch's size.
    assert abs(hy[hy.vintage == 2023].lifetime_p50.iloc[0] / lifetime_bottles("Hypersonic Pinot Noir", 2023) - 1) < 0.10
    # Trend compounds per vintage from the anchor (2023): 2024 = lifetime * trend, 2022 = lifetime / trend.
    hr = prof[(prof.scope == "label") & (prof.key == "Home Ranch Pinot Noir")].iloc[0]
    hr24 = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2024)].iloc[0]
    assert hr24.lifetime_p50 == pytest.approx(max(hr.lifetime * hr.trend, hr24.sold_to_date))
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
    # Home Ranch's anchor vintage is 2023; give it a generous cases figure so ratio < 1.
    vp = pd.DataFrame([["Late Pinot Noir", 2025, 300, False], ["Copy Pinot Noir", 2025, 300, False],
                       ["Home Ranch Pinot Noir", 2023, 1000, False]], columns=fio.EMPTY["vintage_plan"])
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
    prof = prof[~prof.key.str.startswith("Pinot Noir|")]   # Hypersonic now has no profile at all
    lp = pd.DataFrame([[2027, "Hypersonic Pinot Noir", "continue", None, None, None, None, None, None, None]],
                      columns=fio.EMPTY["label_plan"])
    with caplog.at_level(logging.WARNING, logger="forecast"):
        fc, summ = forecast_vintages(prof, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    assert not ((summ.label == "Hypersonic Pinot Noir") & (summ.vintage == 2025)).any()
    assert "Hypersonic Pinot Noir" in caplog.text


def test_tier_profiled_label_lifetime_anchors_on_own_sales():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    lp = pd.DataFrame([[2027, "Hypersonic Pinot Noir", "continue", None, None, None, None, None, None, None]],
                      columns=fio.EMPTY["label_plan"])
    fc, summ = forecast_vintages(prof, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    inc = summ[(summ.label == "Hypersonic Pinot Noir") & (summ.vintage == 2025)].iloc[0]
    assert inc.profile_key == "Pinot Noir|mid"
    assert abs(inc.lifetime_p50 / lifetime_bottles("Hypersonic Pinot Noir", 2025) - 1) < 0.25
    # No first year anywhere: falls to sold_to_date(newest) / tier cumulative share (>= 3 months), else tier lifetime.
    tier = prof[(prof.scope == "tier") & (prof.key == "Pinot Noir|mid")]
    curve = np.array(list(tier.curve))
    vy = v[v.label == "Hypersonic Pinot Noir"].assign(first_year=np.nan)
    newest = vy.sort_values("vintage").iloc[-1]
    expect = newest.sold_to_date / curve[:, :int(newest.months_sold)].sum()
    assert b._own_tier_lifetime("Hypersonic Pinot Noir", vy, curve, 999.0) == pytest.approx(expect)
    assert b._own_tier_lifetime("Hypersonic Pinot Noir", vy.assign(months_sold=2), curve, 999.0) == 999.0
    assert b._own_tier_lifetime("Nobody", vy, curve, 999.0) == 999.0


# --- fix round 1 ----------------------------------------------------------------

def test_selling_vintage_conserves_remaining_and_bands_respect_sold():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    # Plan year 2028 -> window to Dec 2029 covers every remaining month (HR 2024 ends May 2029).
    fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), fio._empty_vintage_plan(), ORIGIN, 2028, P, bands)
    s = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2024)].iloc[0]
    got = fc[(fc.label == "Home Ranch Pinot Noir") & (fc.vintage == 2024)].p50.sum()
    assert got == pytest.approx(s.lifetime_p50 - s.sold_to_date, abs=1e-6)
    selling = summ[summ.sold_to_date > 0]
    assert (selling.lifetime_p10 >= selling.sold_to_date - 1e-9).all()
    assert (selling.lifetime_p10 <= selling.lifetime_p50).all() and (selling.lifetime_p50 <= selling.lifetime_p90).all()
    rem = s.lifetime_p50 - s.sold_to_date
    assert s.lifetime_p10 == pytest.approx(s.sold_to_date + rem * np.exp(-1.2816 * 0.35))
    assert s.lifetime_p90 == pytest.approx(s.sold_to_date + rem * np.exp(1.2816 * 0.35))


def test_sold_at_or_over_cap_gives_zeros():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    vp = pd.DataFrame([["Home Ranch Pinot Noir", 2022, 100, False]], columns=fio.EMPTY["vintage_plan"])   # cap 1200 < sold
    fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), vp, ORIGIN, 2027, P, bands)
    rows = fc[(fc.label == "Home Ranch Pinot Noir") & (fc.vintage == 2022)]
    assert (rows.p50 == 0).all() and (rows.p90 == 0).all()
    s = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2022)].iloc[0]
    assert s.lifetime_p50 == s.sold_to_date == s.lifetime_p10 == s.lifetime_p90


def test_lifetime_p90_capped_at_cases():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    vp = pd.DataFrame([["Ten Oaks Pinot Noir", 2023, 100, False]], columns=fio.EMPTY["vintage_plan"])   # cap 1200
    fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), vp, ORIGIN, 2027, P, bands)
    s = summ[(summ.label == "Ten Oaks Pinot Noir") & (summ.vintage == 2023)].iloc[0]
    assert s.lifetime_p50 == pytest.approx(1200) and s.lifetime_p90 == pytest.approx(1200)


def test_unreleased_vintage_before_origin_is_clamped_with_full_mass(caplog):
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    lp = pd.DataFrame([[2030, "Home Ranch Pinot Noir", "continue", 2025, pd.Timestamp("2026-01-01"), None, None, None, None, None]],
                      columns=fio.EMPTY["label_plan"])
    with caplog.at_level(logging.WARNING, logger="forecast"):
        fc, summ = forecast_vintages(prof, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2030, P, bands)
    s = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2025)].iloc[0]
    assert s.release_month == pd.Timestamp("2026-11-01")
    assert "planned release 2026-01-01 is not after origin; moved to 2026-11-01" in caplog.text
    rows = fc[(fc.label == "Home Ranch Pinot Noir") & (fc.vintage == 2025)]
    assert rows.month_start.min() == pd.Timestamp("2026-11-01")
    assert rows.p50.sum() == pytest.approx(s.lifetime_p50, abs=1e-6)


def test_complete_curve_skips_reference_with_no_cumulative_share():
    slots = np.array([[30.0, 10.0, 0.0, 0.0]])
    dead = np.array([[0.0, 0.0, 0.5, 0.5]])
    good = np.array([[0.25, 0.25, 0.25, 0.25]])
    out, est = b._complete_curve(slots, 2, [dead, good])
    assert est == pytest.approx(80.0) and np.allclose(out, [[30 / 80, 10 / 80, 0.25, 0.25]])
    out, est = b._complete_curve(slots, 2, [dead, None])
    assert est == 40.0


def test_no_trend_compounding_when_curve_has_no_first_year_mass():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P).copy(); bands = price_bands(h)
    hr = prof.key == "Home Ranch Pinot Noir"
    flat = [0.0] * 12 + [1.0 / (24 * 4)] * 24
    prof.loc[hr, "curve"] = pd.Series([list(flat) for _ in range(int(hr.sum()))], index=prof.index[hr])
    lp = pd.DataFrame([[2027, "Home Ranch Pinot Noir", "continue", 2025, None, None, None, None, None, None]],
                      columns=fio.EMPTY["label_plan"])
    fc, summ = forecast_vintages(prof, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    s = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2025)].iloc[0]
    assert s.lifetime_p50 == pytest.approx(float(prof[hr].lifetime.iloc[0]))


def test_backtest_vintages_rows_and_bands():
    hist = make_bottle_history()
    vt = vintage_table(hist, ORIGIN)
    bt = b.backtest_vintages(hist, vt, P)
    assert list(bt.columns) == ["label", "vintage", "first_year_actual", "first_year_forecast", "p10", "p90"]
    hr = bt[(bt["label"] == "Home Ranch Pinot Noir") & (bt["vintage"] == 2023)].iloc[0]
    assert abs(hr.first_year_forecast / hr.first_year_actual - 1) < 0.10
    assert hr.p10 < hr.first_year_forecast < hr.p90
    # one earlier vintage only -> trend 1.0 (forecast == previous first_year)
    h22 = bt[(bt["label"] == "Home Ranch Pinot Noir") & (bt["vintage"] == 2022)].iloc[0]
    assert h22.first_year_forecast == pytest.approx(1919.0)
    # label with a single known first_year (Hypersonic) and the magnum get no rows
    assert bt[bt["label"] == "Hypersonic Pinot Noir"].empty
    assert not bt["label"].str.endswith(" - 1.5L").any()
    # no look-ahead: 2023 forecast uses 2022 and 2021 only
    t = 1 + P.lambda_shrink * (np.clip(2024.0 / 1919.0, *P.trend_clip) - 1)
    assert hr.first_year_forecast == pytest.approx(2024.0 * t)
    # the app can feed it straight into fit_profiles as the dispersion source
    prof = fit_profiles(hist, vt, P, dispersion=bt)
    assert (prof["log_dispersion"] > 0).all()


def test_unreviewed_labels_get_default_next_vintage():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    keys = set(zip(summ.label, summ.vintage))
    # newest vintage < 24 months sold -> newest + 1 is a candidate
    for label, nxt in [("Home Ranch Pinot Noir", 2025), ("Three Hills Pinot Noir", 2025),
                       ("Carneros Chardonnay", 2025), ("Hypersonic Pinot Noir", 2025),
                       ("Ten Oaks Pinot Noir", 2024)]:
        assert (label, nxt) in keys, label
    # Mikado 2022 has sold 29 months: no default 2023
    assert ("Mikado Tree Pinot Noir - 1.5L", 2023) not in keys
    assert not summ.duplicated(["label", "vintage"]).any()
    hr = summ[(summ.label == "Home Ranch Pinot Noir") & (summ.vintage == 2025)].iloc[0]
    assert hr.release_month == pd.Timestamp("2027-06-01") and hr.profile_key == "Home Ranch Pinot Noir"
    assert fc[(fc.label == "Home Ranch Pinot Noir") & (fc.vintage == 2025)].month_start.min() == pd.Timestamp("2027-06-01")
    # Ten Oaks 2023 released 2025-06: default 2024 release (2026-06) is not after origin -> rolled a year
    to = summ[(summ.label == "Ten Oaks Pinot Noir") & (summ.vintage == 2024)].iloc[0]
    assert to.release_month == pd.Timestamp("2027-06-01")


def test_default_candidate_rolls_forward_by_year_and_logs_count(caplog):
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    with caplog.at_level(logging.INFO, logger="forecast"):
        fc, summ = forecast_vintages(prof, v, h, fio._empty_label_plan(), fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    assert "default next-vintage candidates added: 5" in caplog.text
    assert "Ten Oaks Pinot Noir 2024 default release 2026-06-01 is not after origin; rolled to 2027-06-01" in caplog.text
    rows = fc[(fc.label == "Ten Oaks Pinot Noir") & (fc.vintage == 2024)]
    assert rows.month_start.min() == pd.Timestamp("2027-06-01")


def test_default_next_vintage_respects_plan_retire_and_named_incoming():
    h = make_bottle_history(); v = vintage_table(h, ORIGIN); prof = fit_profiles(h, v, P); bands = price_bands(h)
    lp = pd.DataFrame([[2027, "Home Ranch Pinot Noir", "retire", None, None, None, None, None, None, None],
                       [2027, "Three Hills Pinot Noir", "continue", 2026, None, None, None, None, None, None]],
                      columns=fio.EMPTY["label_plan"])
    fc, summ = forecast_vintages(prof, v, h, lp, fio._empty_vintage_plan(), ORIGIN, 2027, P, bands)
    keys = set(zip(summ.label, summ.vintage))
    assert ("Home Ranch Pinot Noir", 2025) not in keys and ("Home Ranch Pinot Noir", 2024) in keys
    assert ("Three Hills Pinot Noir", 2026) in keys and ("Three Hills Pinot Noir", 2025) not in keys
    assert ("Carneros Chardonnay", 2025) in keys
    assert not summ.duplicated(["label", "vintage"]).any()
