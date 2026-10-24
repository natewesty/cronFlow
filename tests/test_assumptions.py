import pandas as pd

from forecast.assumptions import parse_events, parse_params, parse_schedule


def test_parse_schedule_sorted_and_defaults_days():
    sched = parse_schedule({"releases": [
        {"start": "2027-09-01", "days": 2, "label": "Fall"},
        {"start": "2027-02-24", "label": "Spring"},
    ]})
    assert [s.start for s in sched] == [pd.Timestamp("2027-02-24"), pd.Timestamp("2027-09-01")]
    assert sched[0].days == 2 and sched[0].label == "Spring"


def test_parse_schedule_skips_bad_entries():
    sched = parse_schedule({"releases": [
        {"start": "not a date", "days": 2}, {"days": 2}, {"start": "2027-05-05", "days": 0},
        {"start": "2027-05-05", "days": "3"},
    ]})
    assert len(sched) == 1 and sched[0].days == 3 and sched[0].label == ""


def test_parse_schedule_absent_returns_none():
    assert parse_schedule(None) is None
    assert parse_schedule({}) is None
    assert parse_schedule({"releases": []}) is None
    assert parse_schedule({"releases": [{"start": "bad"}]}) is None


def test_parse_events_normalises_numbers():
    ev = parse_events({"events": [
        {"date": "2027-07-24", "label": "Gala", "event_fees": "400000", "guests": 250},
        {"date": "bad"}, "junk",
    ]})
    assert ev == [{"date": "2027-07-24", "label": "Gala", "event_fees": 400000.0, "event_wine": 0.0, "guests": 250.0}]
    assert parse_events(None) == []


def test_parse_params():
    p = parse_params({"lambda_shrink": 0.4, "bogus": 1})
    assert p.lambda_shrink == 0.4 and p.w1 == 0.7
    assert parse_params(None).lambda_shrink == 0.7


def test_parse_params_bad_override_falls_back():
    assert parse_params({"trend_clip": 5}).trend_clip == (0.5, 2.0)


def test_parse_schedule_and_events_skip_pathological():
    sched = parse_schedule({"releases": [{"start": "NaT"}, {"start": "2027-05-05", "days": float("inf")},
                                         {"start": "2027-06-01"}]})
    assert len(sched) == 1
    assert len(parse_events({"events": [{"date": "NaT"}, {"date": "2027-07-24"}]})) == 1
