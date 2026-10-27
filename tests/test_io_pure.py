import json

import numpy as np
import pandas as pd
import pytest

from forecast import io as fio
from forecast.config import ALL_KPIS


def test_database_url_prefers_env_url_and_adds_ssl_for_render(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@dpg-x.oregon-postgres.render.com/db")
    assert fio.database_url() == "postgresql+psycopg2://u:p@dpg-x.oregon-postgres.render.com/db?sslmode=require"
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db?x=1")
    assert fio.database_url() == "postgresql+psycopg2://u:p@localhost/db?x=1"


def test_database_url_falls_back_to_parts(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "h"); monkeypatch.setenv("DB_NAME", "n")
    monkeypatch.setenv("DB_USER", "u"); monkeypatch.setenv("DB_PASSWORD", "p")
    monkeypatch.delenv("DB_PORT", raising=False)
    assert fio.database_url() == "postgresql+psycopg2://u:p@h:5432/n"
    monkeypatch.delenv("DB_HOST")
    with pytest.raises(RuntimeError):
        fio.database_url()


def test_frame_from_rows_fills_gaps():
    origin = pd.Timestamp("2026-01-05")
    rows = [("2026-01-01", 1, 10.0), ("2026-01-03", 1, 30.0), ("2026-01-01", 23, 100.0),
            ("2026-01-04", 23, 104.0), ("2026-01-05", 1, 50.0)]
    df = fio.frame_from_rows(rows, origin)
    assert list(df.columns) == ALL_KPIS
    assert df.index.equals(pd.date_range("2026-01-01", "2026-01-05"))
    assert df[1].tolist() == [10.0, 0.0, 30.0, 0.0, 50.0]
    assert df[23].tolist() == [100.0, 100.0, 100.0, 104.0, 104.0]
    assert (df[2] == 0).all() and not df.isna().any().any()


def test_frame_from_rows_empty():
    df = fio.frame_from_rows([], pd.Timestamp("2026-01-05"))
    assert df.empty and list(df.columns) == ALL_KPIS


def test_daily_records_shape():
    dates = pd.date_range("2026-10-24", periods=2)
    point = pd.DataFrame({1: [10.0, 20.0], 16: [5.0, 6.0]}, index=dates)
    p10 = pd.DataFrame({1: [8.0, np.nan], 16: [np.nan, np.nan]}, index=dates)
    p90 = pd.DataFrame({1: [12.0, np.nan], 16: [np.nan, np.nan]}, index=dates)
    comps = {1: pd.DataFrame({"organic": [10.0, 20.0]}, index=dates),
             16: pd.DataFrame({"organic": [5.0, 6.0], "otb": [7.0, np.nan]}, index=dates)}
    recs = fio.daily_records(point, p10, p90, comps, run_id=7)
    assert len(recs) == 4
    r = next(x for x in recs if x["kpi_id"] == 16 and x["date_key"] == "2026-10-24")
    assert r == {"run_id": 7, "kpi_id": 16, "entity_id": 0, "date_key": "2026-10-24", "horizon": 1,
                 "p50": 5.0, "p10": None, "p90": None, "otb": 7.0,
                 "components": json.dumps({"organic": 5.0})}
    r2 = next(x for x in recs if x["kpi_id"] == 1 and x["horizon"] == 2)
    assert r2["p10"] is None and r2["p90"] is None and r2["otb"] is None


def test_monthly_and_accuracy_records_convert_nan_to_none():
    monthly = pd.DataFrame([{"month_start": pd.Timestamp("2027-01-01"), "kpi_id": 3, "is_actual": False,
                             "organic": np.nan, "organic_neutral": np.nan, "release": np.nan,
                             "event": np.nan, "fitted_growth": np.nan, "p50": 12.5, "release_planned": False}])
    m = fio.monthly_records(monthly, run_id=3)
    assert m[0] == {"run_id": 3, "kpi_id": 3, "entity_id": 0, "month_start": "2027-01-01",
                    "is_actual": False, "organic": None, "organic_neutral": None, "release": None,
                    "event": None, "fitted_growth": None, "p50": 12.5, "release_planned": False}
    acc = pd.DataFrame([{"kpi_id": 9, "horizon_bucket": "1-7", "wmape": np.nan, "bias": np.nan,
                         "ratio_p10": np.nan, "ratio_p90": np.nan, "weekly_wmape": np.nan,
                         "monthly_wmape": np.nan, "n": 56}])
    a = fio.accuracy_records(acc, run_id=3)
    assert a[0]["wmape"] is None and a[0]["n"] == 56 and a[0]["run_id"] == 3


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, stmt, *args, **kwargs):
        return _FakeResult(self._rows)


def test_load_assumptions_tolerates_bad_json():
    conn = _FakeConn([("engine_params", '{"lambda_shrink": 0.4}'), ("events", {"events": []}),
                      ("shipment_schedule", "not json")])
    assert fio.load_assumptions(conn) == {"engine_params": {"lambda_shrink": 0.4}, "events": {"events": []},
                                          "shipment_schedule": "not json"}


def test_expand_values_repeats_tuple_with_indexed_params():
    sql = "insert into s.t (a, b) values (:a, cast(:b as jsonb)) on conflict (a) do update set b = excluded.b"
    out = fio.expand_values(sql, 2)
    assert out == ("insert into s.t (a, b) values (:a_0, cast(:b_0 as jsonb)), (:a_1, cast(:b_1 as jsonb))"
                   " on conflict (a) do update set b = excluded.b")


def test_insert_many_sends_one_statement_per_chunk_and_writes_every_row(monkeypatch):
    from sqlalchemy import create_engine, text
    monkeypatch.setattr(fio, "CHUNK", 3)
    engine = create_engine("sqlite://")
    calls = []
    with engine.begin() as conn:
        conn.execute(text("create table t (a integer, b text)"))
        real = conn.execute
        def spy(stmt, *args, **kw):
            calls.append(str(stmt))
            return real(stmt, *args, **kw)
        monkeypatch.setattr(conn, "execute", spy)
        fio._insert_many(conn, "insert into t (a, b) values (:a, :b)", [{"a": i, "b": str(i)} for i in range(7)])
        monkeypatch.undo()
        rows = conn.execute(text("select a, b from t order by a")).fetchall()
    assert len(calls) == 3                                   # 3 + 3 + 1 rows
    assert [c.count("(:a_") for c in calls] == [3, 3, 1]     # multi-row VALUES, not executemany
    assert rows == [(i, str(i)) for i in range(7)]


def test_database_url_keeps_explicit_driver(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://u:p@localhost/db")
    assert fio.database_url() == "postgresql+psycopg2://u:p@localhost/db"


def test_v2_loader_stubs_return_typed_empty_frames():
    for name in ["club_plan", "tier_shares", "label_plan", "vintage_plan", "bottle_history", "club_orders", "memberships"]:
        assert list(fio.EMPTY[name]) == list(getattr(fio, f"_empty_{name}")().columns)
    assert fio._empty_club_plan().empty


def test_monthly_records_include_release_planned():
    recs = fio.monthly_records(pd.DataFrame([{"month_start": pd.Timestamp("2027-01-01"), "kpi_id": 4, "is_actual": False, "organic": 1, "organic_neutral": 1, "release": 2, "event": 0, "fitted_growth": 1, "p50": 3, "release_planned": True}]), run_id=1)
    assert recs[0]["release_planned"] is True


# --- v2 converters -----------------------------------------------------------------

def test_period_and_accuracy_period_records():
    from forecast.backtest import ACCURACY_PERIOD_COLUMNS, PERIOD_COLUMNS
    per = pd.DataFrame([[1, "week", 2, pd.Timestamp("2026-09-07"), pd.Timestamp("2026-08-30"), 5.0, np.nan, 1.0, 9.0]],
                       columns=PERIOD_COLUMNS)
    rec = fio.period_records(per, 7)
    assert len(rec) == 1 and rec[0]["run_id"] == 7 and rec[0]["period_start"] == "2026-09-07"
    assert rec[0]["origin_date"] == "2026-08-30" and rec[0]["forecast"] is None
    acc = pd.DataFrame([[1, "month", 1, 0.2, np.nan, 0.5, 4]], columns=ACCURACY_PERIOD_COLUMNS)
    r = fio.accuracy_period_records(acc, 7)[0]
    assert r["bias"] is None and r["n"] == 4 and r["hit_rate"] == 0.5


def test_bottle_records():
    fc = pd.DataFrame([["Lux", 2022, pd.Timestamp("2026-11-01"), "Club", 3.0, np.nan, 5.0]],
                      columns=["label", "vintage", "month_start", "channel", "p50", "p10", "p90"])
    r = fio.bottle_records(fc, 3)[0]
    assert r["month_start"] == "2026-11-01" and r["p10"] is None and r["vintage"] == 2022
    summ = pd.DataFrame([["Lux", 2022, "Pinot Noir", pd.Timestamp("2024-03-01"), 120.0, np.nan, 200.0, 150.0, 250.0, "k"]],
                        columns=["label", "vintage", "varietal", "release_month", "sold_to_date", "cases_made",
                                 "lifetime_p50", "lifetime_p10", "lifetime_p90", "profile_key"])
    s = fio.vintage_summary_records(summ, 3)
    assert len(s) == 1 and s[0]["release_month"] == "2024-03-01" and s[0]["cases_made"] is None
    assert s[0]["sold_to_date"] == 120 and s[0]["profile_key"] == "k"
    bt = pd.DataFrame([["Lux", 2022, 10.0, np.nan, 5.0, 15.0]],
                      columns=["label", "vintage", "first_year_actual", "first_year_forecast", "p10", "p90"])
    b = fio.bottle_backtest_records(bt, 3)[0]
    assert b["first_year_forecast"] is None and b["first_year_actual"] == 10.0


def test_profile_records_curve_json():
    curve = np.linspace(0.0, 0.1, 36)
    prof = pd.DataFrame([["label", "Lux", "Club", curve, 100.0, 0.02, np.nan, 3]],
                        columns=["scope", "key", "channel", "curve", "lifetime", "trend", "log_dispersion", "n_vintages"])
    r = fio.profile_records(prof, 9)[0]
    got = json.loads(r["curve"])
    assert len(got) == 36 and all(isinstance(x, float) for x in got)
    assert r["log_dispersion"] is None and r["n_vintages"] == 3


def test_club_history_records_nil_uuid_for_other():
    from forecast.clubs import HISTORY_COLUMNS
    tid = "11111111-1111-1111-1111-111111111111"
    h = pd.DataFrame([[pd.Timestamp("2026-04-01"), "spring", tid, "Gold", "Lux", 2022, 2.0, 40.0, 10, 20, 0.5, 20, 800.0],
                      [pd.Timestamp("2026-04-01"), "spring", "other", "Other", "Lux", np.nan, np.nan, np.nan, 3, 0, np.nan, 3, 90.0]],
                     columns=HISTORY_COLUMNS)
    recs = fio.club_history_records(h)
    assert len(recs) == 2
    assert recs[0]["tier_id"] == tid and recs[0]["release_start"] == "2026-04-01" and recs[0]["vintage"] == 2022
    assert recs[1]["tier_id"] == fio.NIL_UUID and recs[1]["vintage"] is None and recs[1]["take_rate"] is None


def test_bottle_history_sql_includes_origin_day():
    assert "interval '1 day'" in fio.BOTTLE_HISTORY_SQL and "<= :origin" not in fio.BOTTLE_HISTORY_SQL


class _RecordingConn:
    def __init__(self):
        self.calls = []

    def execute(self, stmt, params=None):
        self.calls.append((" ".join(str(stmt).split()), params))


def test_prune_runs_deletes_v2_detail_of_old_non_latest_runs():
    stmts = fio.prune_detail_statements()
    assert len(stmts) == len(fio.DETAIL_TABLES) == 6
    assert set(fio.DETAIL_TABLES) == {"forecast_backtest_periods", "forecast_accuracy_periods", "bottle_forecast",
                                      "bottle_profiles", "bottle_backtest", "bottle_vintage_summary"}
    for table, sql in zip(fio.DETAIL_TABLES, stmts):
        flat = " ".join(sql.split())
        assert flat.startswith(f"delete from forecast.{table} d using forecast.forecast_runs r")
        assert "d.run_id = r.run_id and not r.is_latest" in flat
        assert "r.started_at < now() - make_interval(days => :detail_days)" in flat
    conn = _RecordingConn()
    fio.prune_runs(conn)
    assert len(conn.calls) == 7
    assert "delete from forecast.forecast_runs" in conn.calls[0][0] and conn.calls[0][1] == {"days": 400}
    assert all(p == {"detail_days": 14} for _, p in conn.calls[1:])
    conn = _RecordingConn()
    fio.prune_runs(conn, keep_days_detail=3)
    assert all(p == {"detail_days": 3} for _, p in conn.calls[1:])


def test_finish_run_writes_bottle_rows_when_given():
    conn = _RecordingConn()
    fio.finish_run(conn, 7, "success", bottle_rows=123)
    sql, params = conn.calls[-1]
    assert sql.startswith("update forecast.forecast_runs set status = :status")
    assert "is_latest = :latest, bottle_rows = :bottle_rows where run_id = :run_id" in sql
    assert params == {"status": "success", "error": None, "latest": True, "run_id": 7, "bottle_rows": 123}
    conn = _RecordingConn()
    fio.finish_run(conn, 7, "failed", "boom")
    sql, params = conn.calls[-1]
    assert "bottle_rows" not in sql and "bottle_rows" not in params and not params["latest"]


def test_club_orders_frame_drops_refund_only_orders():
    ts = pd.Timestamp("2026-09-03 10:00")
    rows = [("o1", "c1", ts, "2023 Home Ranch Pinot Noir", 2023, 2, 120.0),
            ("o1", "c1", ts, "2022 Carneros Chardonnay", 2022, 1, 48.0),
            ("o2", "c2", ts, "2023 Home Ranch Pinot Noir", 2023, -2, -120.0),     # refund only
            ("o3", "c3", ts, "2023 Home Ranch Pinot Noir", 2023, 0, 0.0),         # nets to zero
            ("o4", "c4", ts, "2023 Home Ranch Pinot Noir", 2023, 3, 180.0),
            ("o4", "c4", ts, "2022 Carneros Chardonnay", 2022, -1, -48.0),        # partial refund kept
            ("o5", None, ts, "2023 Home Ranch Pinot Noir", 2023, 2, 120.0)]
    df = fio.club_orders_frame(rows)
    assert list(df.columns) == fio.EMPTY["club_orders"]
    assert sorted(df.order_id.unique()) == ["o1", "o4"]
    assert len(df[df.order_id == "o4"]) == 2 and df[df.order_id == "o4"].bottles.sum() == 2
    assert fio.club_orders_frame(rows[2:4]).empty and list(fio.club_orders_frame([]).columns) == fio.EMPTY["club_orders"]
