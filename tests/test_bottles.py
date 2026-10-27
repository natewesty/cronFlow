import numpy as np
import pandas as pd

from forecast import bottles as b
from tests.synth_bottles import make_bottle_history, lifetime_bottles

ORIGIN = pd.Timestamp("2026-10-25")


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
