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


V2_TABLES = ["forecast_backtest_periods", "forecast_accuracy_periods", "bottle_forecast", "bottle_vintage_summary",
             "bottle_profiles", "bottle_backtest", "bottle_vintage_plan", "label_plan",
             "club_release_history", "club_release_plan", "club_release_tier_plan"]


def test_v2_tables_present_and_idempotent():
    sql = SCHEMA.read_text()
    for t in V2_TABLES:
        assert f"create table if not exists forecast.{t}" in sql
    assert "add column if not exists release_planned" in sql
    assert "add column if not exists bottle_rows" in sql
    stmts = [s for s in sql.split(";") if s.strip()]
    assert all("if not exists" in s.lower() or s.strip().lower().startswith("create schema") for s in stmts)


def test_bottle_vintage_summary_profile_key_alter():
    sql = " ".join(SCHEMA.read_text().lower().split())
    assert "alter table forecast.bottle_vintage_summary add column if not exists profile_key text" in sql


def test_v3_schema_statements():
    sql = SCHEMA.read_text()
    assert "create table if not exists forecast.forecast_models" in sql
    assert "candidates jsonb not null" in sql
    assert ("alter table forecast.forecast_backtest_periods "
            "add column if not exists calendar boolean not null default false") in sql
    assert "alter table forecast.forecast_accuracy_periods add column if not exists mae numeric" in sql
    from forecast import io as fio
    assert "forecast_models" in fio.DETAIL_TABLES


V4_TABLES = ["bottle_history", "stock_snapshot", "stock_snapshot_row", "wine_code_map", "stock_plan",
             "allocation_plan", "channel_share"]


def test_schema_defines_bottles_v2_tables():
    sql = SCHEMA.read_text().lower()
    for t in V4_TABLES:
        assert re.search(rf"create table if not exists forecast\.{t}\b", sql), t
    assert "primary key (label, vintage, month_start, channel)" in sql
    assert "release_date date" in sql
