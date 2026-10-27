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
                             "event": np.nan, "fitted_growth": np.nan, "p50": 12.5}])
    m = fio.monthly_records(monthly, run_id=3)
    assert m[0] == {"run_id": 3, "kpi_id": 3, "entity_id": 0, "month_start": "2027-01-01",
                    "is_actual": False, "organic": None, "organic_neutral": None, "release": None,
                    "event": None, "fitted_growth": None, "p50": 12.5}
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
