import numpy as np
import pandas as pd

from forecast.models.events import cap_series, event_component
from forecast.models.otb import otb_series


def test_cap_series_caps_at_percentile_of_nonzero_days():
    idx = pd.date_range("2026-01-01", periods=100)
    y = pd.Series(0.0, index=idx)
    y.iloc[:20] = np.arange(1, 21, dtype=float)      # nonzero days 1..20, p95 = 19.05
    capped = cap_series(y, 0.95)
    assert capped.max() == float(np.quantile(np.arange(1, 21), 0.95))
    assert (capped.iloc[20:] == 0).all()
    assert cap_series(pd.Series(0.0, index=idx), 0.95).eq(0).all()


def test_event_component_adds_amounts_on_dates_inside_horizon():
    dates = pd.date_range("2027-07-20", periods=10)
    events = [
        {"date": "2027-07-24", "label": "Gala", "event_fees": 400000, "event_wine": 20000, "guests": 250},
        {"date": "2027-07-24", "label": "Second", "event_fees": 1000},
        {"date": "2027-09-01", "label": "Outside", "event_fees": 99},
        {"date": "not-a-date", "label": "Bad", "event_fees": 5},
    ]
    fees = event_component(events, "event_fees", dates)
    assert fees[pd.Timestamp("2027-07-24")] == 401000 and fees.sum() == 401000
    assert event_component(events, "guests", dates).sum() == 250
    assert event_component([], "event_fees", dates).sum() == 0


def test_otb_series_filters_attribution_and_sums_by_date():
    otb = pd.DataFrame({
        "reservation_date": pd.to_datetime(["2026-10-25", "2026-10-25", "2026-10-26"]),
        "attribution": ["Tasting Room", "Event", "Tasting Room"],
        "reservations": [10, 2, 5], "guests": [40, 50, 20], "fees": [4000.0, 9000.0, 2000.0],
    })
    dates = pd.date_range("2026-10-24", periods=4)
    all_guests = otb_series(otb, "guests", None, dates)
    assert np.isnan(all_guests[pd.Timestamp("2026-10-24")])
    assert all_guests[pd.Timestamp("2026-10-25")] == 90
    tr_fees = otb_series(otb, "fees", "Tasting Room", dates)
    assert tr_fees[pd.Timestamp("2026-10-25")] == 4000 and tr_fees[pd.Timestamp("2026-10-26")] == 2000
    assert otb_series(None, "guests", None, dates).isna().all()
