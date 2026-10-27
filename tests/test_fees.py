import pandas as pd
import pytest

from forecast.config import EngineParams
from forecast.models.fees import fee_component
from forecast.types import Release, ScheduledRelease

P = EngineParams()


def test_fee_component_places_amounts_at_matched_offsets():
    idx = pd.date_range("2025-01-01", "2026-10-25")
    y = pd.Series(0.0, index=idx); y["2026-05-16"] = 36226; y["2026-05-17"] = 28994
    rel = [Release(start=pd.Timestamp("2026-05-06"), days=2, day_totals=[1, 1])]
    s = ScheduledRelease(start=pd.Timestamp("2027-05-05"), days=2, label="", season="summer")
    members = pd.Series(1000.0, index=idx); stock = pd.Series(1100.0, index=pd.date_range("2026-10-26", "2027-12-31"))
    dates = pd.date_range("2027-04-01", "2027-06-30")
    out = fee_component(y, rel, [s], members, stock, dates, P)
    assert out.loc["2027-05-15"] == pytest.approx(36226 * 1.1) and out.loc["2027-05-16"] == pytest.approx(28994 * 1.1)
    assert (out != 0).sum() == 2


def test_fee_component_no_match_is_zero():
    idx = pd.date_range("2026-01-01", "2026-10-25"); y = pd.Series(0.0, index=idx)
    s = ScheduledRelease(start=pd.Timestamp("2027-05-05"), days=2, label="", season="summer")
    out = fee_component(y, [], [s], pd.Series(1.0, index=idx), pd.Series(1.0, index=idx), pd.date_range("2027-05-01", periods=30), P)
    assert out.sum() == 0
