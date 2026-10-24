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
