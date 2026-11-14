"""Publishing the Calculation Record and checking the Tock walk.

Every full run publishes the entries (with whether each still matches the code), the pipeline-wide
events and the cron schedule to the warehouse, where the dashboard reads them. After dbt, the run
checks that the Tock walk's last line still equals the dashboard's value. Neither may ever fail a run.
"""
import json
from pathlib import Path

import pytest
import yaml

from metrics import publish, record, walk

REPO = Path(__file__).resolve().parents[1]


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class Conn:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, params=None):
        self.engine.calls.append((str(statement), params))
        return Rows(self.engine.rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Engine:
    def __init__(self, rows=()):
        self.calls, self.rows, self.transactions = [], list(rows), 0

    def begin(self):
        self.transactions += 1
        return Conn(self)

    connect = begin


class BrokenEngine:
    def begin(self):
        raise RuntimeError("database is down")

    connect = begin


@pytest.fixture
def repo(tmp_path):
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.sql").write_text("select 1\n")
    (tmp_path / "metrics" / "entries").mkdir(parents=True)
    entry = {"id": "tasting-fees", "name": "Tasting Fees", "kpi_id": 2, "group": "Tasting Room",
             "shown_on": "Dashboard", "summary": "What guests paid.", "calculation": ["Add it up."],
             "includes": [], "excludes": [], "counted_on": "Visit day.", "sources": ["Tock"],
             "history": [{"date": "2025-10-07", "change": "Changed.", "effect": "Lower."}],
             "models": ["models/a.sql"]}
    (tmp_path / "metrics" / "entries" / "tasting-fees.yml").write_text(yaml.safe_dump(entry, sort_keys=False))
    (tmp_path / "metrics" / "events.yml").write_text(yaml.safe_dump(
        [{"date": "2026-07-01", "change": "Fiscal year.", "effect": "Totals moved.", "metrics": "all"}]))
    (tmp_path / "metrics" / "confirmed.json").write_text("{}\n")
    (tmp_path / "render.yaml").write_text(yaml.safe_dump({"services": [
        {"type": "cron", "name": "cronDon", "schedule": "0 1,13 * * *"},
        {"type": "cron", "name": "cronDon-forecast", "schedule": "0 14 * * *"}]}))
    record.confirm(["tasting-fees"], "2026-11-13", tmp_path)
    return tmp_path


def test_read_schedule_from_render_yaml():
    assert publish.read_schedule(REPO) == {"cron": "0 1,13 * * *", "timezone": "UTC"}


def test_read_schedule_is_none_when_the_service_is_missing(tmp_path):
    (tmp_path / "render.yaml").write_text("services: []\n")
    assert publish.read_schedule(tmp_path) is None
    assert publish.read_schedule(tmp_path / "nowhere") is None


def test_build_payload_carries_the_published_entry_shape(repo):
    payload = publish.build_payload(repo)
    assert payload["schedule"] == {"cron": "0 1,13 * * *", "timezone": "UTC"}
    assert payload["events"] == [{"date": "2026-07-01", "change": "Fiscal year.", "effect": "Totals moved.", "metrics": "all"}]
    entry = payload["entries"][0]
    assert set(entry) == {"id", "name", "kpi_id", "group", "shown_on", "summary", "calculation", "includes",
                          "excludes", "counted_on", "sources", "will_not_match", "history", "walk",
                          "confirmed_date", "fingerprint_ok", "changed_files"}
    assert entry["will_not_match"] == [] and entry["walk"] is None        # optional fields are always present
    assert entry["confirmed_date"] == "2026-11-13" and entry["fingerprint_ok"] is True
    json.dumps(payload)                                                    # dates are text, not date objects


def test_build_payload_marks_stale_entries(repo):
    (repo / "models" / "a.sql").write_text("select 2\n")
    entry = publish.build_payload(repo)["entries"][0]
    assert entry["fingerprint_ok"] is False
    assert entry["changed_files"] == ["models/a.sql"]


def test_publish_replaces_record_in_one_transaction(repo):
    engine = Engine()
    assert publish.publish(engine, publish.build_payload(repo), git_sha="abc123") is True
    assert engine.transactions == 1
    statements = [sql for sql, _ in engine.calls]
    delete_at = next(i for i, sql in enumerate(statements) if "DELETE FROM calculation_record" in sql)
    insert_at = next(i for i, sql in enumerate(statements) if "INSERT INTO calculation_record " in sql)
    assert delete_at < insert_at
    insert_params = engine.calls[insert_at][1]
    assert insert_params["entry_id"] == "tasting-fees"
    assert json.loads(insert_params["entry"])["name"] == "Tasting Fees"
    meta_sql, meta_params = next((sql, p) for sql, p in engine.calls if "INSERT INTO calculation_record_meta" in sql)
    assert "ON CONFLICT (id) DO UPDATE" in meta_sql
    assert meta_params["git_sha"] == "abc123"
    assert json.loads(meta_params["schedule"]) == {"cron": "0 1,13 * * *", "timezone": "UTC"}


def test_publish_never_raises(repo):
    assert publish.publish(BrokenEngine(), publish.build_payload(repo)) is False
    assert publish.publish(None, publish.build_payload(repo)) is False


def test_the_real_record_builds_with_all_31_entries():
    payload = publish.build_payload(REPO)
    assert len(payload["entries"]) == 31
    assert all(e["fingerprint_ok"] for e in payload["entries"])
    assert not any("models" in e or "_file" in e for e in payload["entries"])


# ---- the walk check ----

def test_tie_sql_is_the_dbt_test_against_the_built_tables():
    sql = walk.tie_sql()
    assert "{{" not in sql
    assert "from rec_tock_revenue_daily" in sql and "from agg_daily_revenue" in sql


def test_check_walk_returns_none_when_the_dbt_test_file_is_missing(monkeypatch, tmp_path):
    # The comparison is read from tests/assert_tock_walk_ties.sql at run time; a deploy without that
    # file must cost the check, never the import of ingest.py or the run.
    monkeypatch.setattr(walk, "TEST_FILE", tmp_path / "gone.sql")
    assert walk.check_walk(Engine(rows=[])) is None


def test_check_walk_ok_when_no_day_differs():
    assert walk.check_walk(Engine(rows=[])) == {"ok": True, "mismatched_days": 0, "first_mismatch": None}


def test_check_walk_reports_mismatch():
    from datetime import date
    engine = Engine(rows=[(date(2026, 10, 3), 12.5, 0, 0), (date(2026, 9, 1), 0, 4.0, 0)])
    assert walk.check_walk(engine) == {"ok": False, "mismatched_days": 2, "first_mismatch": "2026-09-01"}


def test_check_walk_returns_none_when_query_fails():
    assert walk.check_walk(BrokenEngine()) is None
    assert walk.check_walk(None) is None
