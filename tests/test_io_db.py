import os

import pandas as pd
import pytest
from sqlalchemy import text

from forecast import io as fio

URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL not set")


def test_schema_and_run_lifecycle_round_trip():
    engine = fio.get_engine(URL)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            fio.ensure_schema(conn)
            fio.ensure_schema(conn)                         # idempotent
            run_id = fio.start_run(conn, pd.Timestamp("2026-10-23"), 3, {"engine": {"horizon": 3}})
            dates = pd.date_range("2026-10-24", periods=3)
            point = pd.DataFrame({1: [1.0, 2.0, 3.0]}, index=dates)
            nan = pd.DataFrame({1: [None] * 3}, index=dates, dtype=float)
            fio.write_results(conn, run_id,
                              fio.daily_records(point, nan, nan, {1: pd.DataFrame({"organic": [1.0, 2.0, 3.0]}, index=dates)}, run_id),
                              [], [])
            fio.finish_run(conn, run_id, "success")
            latest = conn.execute(text("select run_id from forecast.forecast_runs where is_latest")).fetchall()
            assert latest == [(run_id,)]
            n = conn.execute(text("select count(*) from forecast.forecast_daily where run_id = :r"), {"r": run_id}).scalar()
            assert n == 3
        finally:
            trans.rollback()


def test_v2_writers_and_club_plan_default_take_rate():
    from forecast.backtest import ACCURACY_PERIOD_COLUMNS, PERIOD_COLUMNS
    from forecast.clubs import HISTORY_COLUMNS
    tid = "11111111-1111-1111-1111-111111111111"
    engine = fio.get_engine(URL)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            fio.ensure_schema(conn)
            run_id = fio.start_run(conn, pd.Timestamp("2026-10-23"), 3, {})
            per = pd.DataFrame([[1, "week", 1, pd.Timestamp("2026-09-07"), pd.Timestamp("2026-09-01"), 5.0, 4.0, 3.0, 6.0,
                                 False]], columns=PERIOD_COLUMNS)
            acc = pd.DataFrame([[1, "week", 1, 0.2, 0.0, 0.5, 4, 1.0]], columns=ACCURACY_PERIOD_COLUMNS)
            fio.write_periods(conn, run_id, fio.period_records(per, run_id), fio.accuracy_period_records(acc, run_id))
            fc = pd.DataFrame([["Lux", 2022, pd.Timestamp("2026-11-01"), "Club", 3.0, 2.0, 5.0]],
                              columns=["label", "vintage", "month_start", "channel", "p50", "p10", "p90"])
            prof = pd.DataFrame([["label", "Lux", "Club", [0.1] * 36, 100.0, 0.0, 0.3, 2]],
                                columns=["scope", "key", "channel", "curve", "lifetime", "trend", "log_dispersion", "n_vintages"])
            fio.write_bottles(conn, run_id, fio.bottle_records(fc, run_id), [], fio.profile_records(prof, run_id), [])
            for table, n in [("forecast_backtest_periods", 1), ("forecast_accuracy_periods", 1),
                             ("bottle_forecast", 1), ("bottle_profiles", 1)]:
                assert conn.execute(text(f"select count(*) from forecast.{table} where run_id = :r"), {"r": run_id}).scalar() == n

            hist = pd.DataFrame([[pd.Timestamp("2026-04-01"), "spring", tid, "Gold", "Lux", 2022, 2.0, 40.0, 10, 20, 0.5, 20, 800.0],
                                 [pd.Timestamp("2026-04-01"), "spring", "other", "Other", "Lux", 2022, 1.0, 40.0, 3, 0, None, 3, 120.0]],
                                columns=HISTORY_COLUMNS)
            fio.write_club_history(conn, fio.club_history_records(hist))
            fio.write_club_history(conn, fio.club_history_records(hist))      # delete + insert is repeatable
            assert conn.execute(text("select count(*) from forecast.club_release_history")).scalar() == 2

            conn.execute(text("""insert into forecast.club_release_plan (plan_year, season, release_date, tier_id, label, vintage, qty)
                                 values (2027, 'spring', '2027-04-01', :t, 'Lux', 2023, 2)"""), {"t": tid})
            plan = fio.load_club_plan(conn, 2027)
            assert list(plan.columns) == fio.EMPTY["club_plan"] and len(plan) == 1
            assert plan.loc[0, "take_rate"] == pytest.approx(0.5)     # from history
            assert plan.loc[0, "price"] == pytest.approx(40.0)        # label's last known price
        finally:
            trans.rollback()


def test_prune_runs_drops_old_v2_detail_but_keeps_latest_and_recent():
    engine = fio.get_engine(URL)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            fio.ensure_schema(conn)
            fc = pd.DataFrame([["Lux", 2022, pd.Timestamp("2026-11-01"), "Club", 3.0, 2.0, 5.0]],
                              columns=["label", "vintage", "month_start", "channel", "p50", "p10", "p90"])
            ids = []
            for _ in range(3):
                rid = fio.start_run(conn, pd.Timestamp("2026-10-23"), 3, {})
                fio.write_bottles(conn, rid, fio.bottle_records(fc, rid), [], [], [])
                fio.finish_run(conn, rid, "success", bottle_rows=1)
                ids.append(rid)
            old, recent, latest = ids
            # old and latest both started 20 days ago; only `latest` is the live run
            conn.execute(text("update forecast.forecast_runs set started_at = now() - interval '20 days' "
                              "where run_id in (:a, :b)"), {"a": old, "b": latest})
            fio.prune_runs(conn)
            n = {r: conn.execute(text("select count(*) from forecast.bottle_forecast where run_id = :r"),
                                 {"r": r}).scalar() for r in ids}
            assert n == {old: 0, recent: 1, latest: 1}
            runs = conn.execute(text("select run_id, bottle_rows from forecast.forecast_runs where run_id in (:a, :b, :c)"),
                                {"a": old, "b": recent, "c": latest}).fetchall()
            assert sorted(runs) == sorted((r, 1) for r in ids)        # run rows stay (400-day rule)
        finally:
            trans.rollback()


def test_v3_models_and_period_columns_round_trip():
    from forecast.backtest import ACCURACY_PERIOD_COLUMNS, PERIOD_COLUMNS
    from forecast.types import Champion
    engine = fio.get_engine(URL)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            fio.ensure_schema(conn)
            run_id = fio.start_run(conn, pd.Timestamp("2026-10-23"), 3, {})
            champ = Champion(w_swm=0.25, calibration=0.95, model="blend", wmape=0.3, wmape_default=0.5,
                             wmape_selection=0.33, n=11, candidates=[{"w": 0.25, "cal": True, "wmape": 0.33}])
            fio.write_models(conn, run_id, fio.model_records({8: champ}, run_id))
            per = pd.DataFrame([[8, "month", 1, pd.Timestamp("2026-09-01"), pd.Timestamp("2026-08-31"),
                                 100.0, 90.0, 80.0, 110.0, True]], columns=PERIOD_COLUMNS)
            acc = pd.DataFrame([[8, "month", 1, 0.1, -0.1, 1.0, 1, 10.0]], columns=ACCURACY_PERIOD_COLUMNS)
            fio.write_periods(conn, run_id, fio.period_records(per, run_id), fio.accuracy_period_records(acc, run_id))
            row = conn.execute(text("select kpi_id, model, w_swm, calibration, wmape, wmape_default, wmape_selection, "
                                    "n, candidates from forecast.forecast_models where run_id = :r"),
                               {"r": run_id}).fetchall()
            assert len(row) == 1
            k, model, w, cal, wm, wd, ws, n, cands = row[0]
            assert (k, model, n) == (8, "blend", 11)
            assert [float(v) for v in (w, cal, wm, wd, ws)] == pytest.approx([0.25, 0.95, 0.3, 0.5, 0.33])
            assert cands == [{"w": 0.25, "cal": True, "wmape": 0.33}]
            cal_flag = conn.execute(text("select calendar from forecast.forecast_backtest_periods where run_id = :r"),
                                    {"r": run_id}).scalar()
            mae = conn.execute(text("select mae from forecast.forecast_accuracy_periods where run_id = :r"),
                               {"r": run_id}).scalar()
            assert cal_flag is True and float(mae) == pytest.approx(10.0)
        finally:
            trans.rollback()
