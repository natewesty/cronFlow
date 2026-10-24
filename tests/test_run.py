from contextlib import contextmanager

import pandas as pd
import pytest

from forecast import run as frun
from forecast.config import ALL_KPIS, EngineParams
from tests.synth import make_history

ORIGIN = pd.Timestamp("2026-10-23")


class FakeConn:
    pass


class FakeEngine:
    @contextmanager
    def begin(self):
        yield FakeConn()

    @contextmanager
    def connect(self):
        yield FakeConn()


@pytest.fixture
def fake_io(monkeypatch):
    """Stub every io function; record what gets written."""
    hist = make_history()
    state = {"written": None, "runs": [], "finished": [], "snapshots": []}
    monkeypatch.setattr(frun.fio, "get_engine", lambda url=None: FakeEngine())
    monkeypatch.setattr(frun.fio, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(frun.fio, "load_history", lambda conn, origin: hist.loc[:origin])
    monkeypatch.setattr(frun.fio, "load_otb", lambda conn, origin: pd.DataFrame(
        columns=["reservation_date", "attribution", "reservations", "guests", "fees"]))
    monkeypatch.setattr(frun.fio, "load_members_at", lambda conn, origin: 3500.0)
    monkeypatch.setattr(frun.fio, "load_assumptions", lambda conn: {})
    monkeypatch.setattr(frun.fio, "start_run", lambda conn, origin, horizon, params: state["runs"].append(params) or 42)
    monkeypatch.setattr(frun.fio, "write_results", lambda conn, run_id, d, m, a: state.__setitem__("written", (run_id, d, m, a)))
    monkeypatch.setattr(frun.fio, "finish_run", lambda conn, run_id, status, error=None: state["finished"].append((run_id, status, error)))
    monkeypatch.setattr(frun.fio, "write_bookings_snapshot", lambda conn, d, otb: state["snapshots"].append(d))
    monkeypatch.setattr(frun.fio, "prune_runs", lambda conn, keep_days=400: None)
    monkeypatch.setattr(frun, "today_pacific", lambda: ORIGIN + pd.Timedelta(days=1))
    return state


def test_today_pacific_is_normalized():
    t = frun.today_pacific()
    assert t.tzinfo is None and t == t.normalize()


def test_dry_run_computes_but_does_not_write(fake_io):
    s = frun.run(origin=ORIGIN, horizon=30, dry_run=True, skip_backtest=True)
    assert s.run_id is None and s.horizon == 30 and s.kpis == len(ALL_KPIS)
    assert s.daily_rows == 30 * len(ALL_KPIS) and s.monthly_rows == 36 * len(ALL_KPIS)
    assert s.accuracy_rows == 0 and s.next28_total_revenue > 0
    assert fake_io["written"] is None and fake_io["runs"] == []


def test_run_writes_and_marks_latest(fake_io):
    s = frun.run(origin=ORIGIN, horizon=30, skip_backtest=True)
    assert s.run_id == 42
    run_id, daily, monthly, accuracy = fake_io["written"]
    assert run_id == 42 and len(daily) == 30 * len(ALL_KPIS) and len(monthly) == 36 * len(ALL_KPIS)
    assert daily[0]["horizon"] == 1 and daily[0]["date_key"] == "2026-10-24"
    assert fake_io["finished"] == [(42, "success", None)]
    assert fake_io["snapshots"] == [ORIGIN + pd.Timedelta(days=1)]
    params = fake_io["runs"][0]
    assert params["engine"]["horizon"] == 30
    assert len(params["schedule"]) == 8 and params["events"] == []


def test_run_uses_history_members_when_table_empty(fake_io, monkeypatch):
    monkeypatch.setattr(frun.fio, "load_members_at", lambda conn, origin: None)
    hist = make_history()
    s = frun.run(origin=ORIGIN, horizon=5, skip_backtest=True)
    _, daily, _, _ = fake_io["written"]
    first23 = next(r for r in daily if r["kpi_id"] == 23 and r["horizon"] == 1)
    assert abs(first23["p50"] - float(hist.loc[ORIGIN, 23])) < 50


def test_run_marks_failed_on_write_error(fake_io, monkeypatch):
    def boom(conn, run_id, d, m, a):
        raise RuntimeError("disk full")
    monkeypatch.setattr(frun.fio, "write_results", boom)
    with pytest.raises(RuntimeError):
        frun.run(origin=ORIGIN, horizon=5, skip_backtest=True)
    assert fake_io["finished"] == [(42, "failed", "disk full")]


def test_run_with_backtest_produces_accuracy_and_bands(fake_io, monkeypatch):
    monkeypatch.setattr(frun.fio, "load_assumptions",
                        lambda conn: {"engine_params": {"backtest_days": 60, "backtest_step": 30}})
    s = frun.run(origin=ORIGIN, horizon=5)
    _, daily, _, accuracy = fake_io["written"]
    assert s.accuracy_rows == len(accuracy) > 0
    kpi1 = next(r for r in daily if r["kpi_id"] == 1 and r["horizon"] == 1)
    assert kpi1["p10"] is not None and kpi1["p10"] < kpi1["p50"] < kpi1["p90"]


def test_run_rejects_future_origin(fake_io):
    with pytest.raises(ValueError):
        frun.run(origin=ORIGIN + pd.Timedelta(days=1), horizon=5, skip_backtest=True)
    assert fake_io["runs"] == []


def test_run_marks_failed_on_compute_error(fake_io, monkeypatch):
    monkeypatch.setattr(frun, "detect_releases", lambda club, params: [])
    monkeypatch.setattr(frun, "default_schedule", lambda releases, origin, years=2: [])

    def boom(*args, **kwargs):
        raise RuntimeError("model boom")
    monkeypatch.setattr(frun, "forecast_daily", boom)
    with pytest.raises(RuntimeError, match="model boom"):
        frun.run(origin=ORIGIN, horizon=5, skip_backtest=True)
    assert fake_io["finished"] == [(42, "failed", "model boom")]
    assert fake_io["written"] is None


def test_backfill_skips_bookings_snapshot(fake_io):
    frun.run(origin=ORIGIN - pd.Timedelta(days=7), horizon=5, skip_backtest=True)
    assert fake_io["snapshots"] == []
    assert fake_io["finished"][-1][1] == "success"
