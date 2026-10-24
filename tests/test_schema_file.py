import re
from pathlib import Path

import pandas as pd

from forecast.types import Context, Release, ScheduledRelease
from forecast.config import EngineParams

SCHEMA = Path(__file__).resolve().parents[1] / "forecast" / "schema.sql"


def test_schema_defines_all_tables():
    sql = SCHEMA.read_text().lower()
    for t in ["forecast_runs", "forecast_daily", "forecast_monthly",
              "forecast_accuracy", "assumptions", "bookings_snapshot"]:
        assert re.search(rf"create table if not exists forecast\.{t}\b", sql), t
    assert "create schema if not exists forecast" in sql


def test_types_construct():
    r = Release(start=pd.Timestamp("2026-09-02"), days=2, day_totals=[300000.0, 1100000.0])
    s = ScheduledRelease(start=pd.Timestamp("2027-09-01"), days=2, label="Sep +1y")
    ctx = Context(params=EngineParams(), releases=[r], schedule=[s], events=[], otb=None, members_a0=3588.0)
    assert ctx.schedule[0].days == 2 and ctx.releases[0].total == 1400000.0
