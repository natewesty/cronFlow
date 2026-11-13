"""Run log.

Nothing recorded pipeline runs before this. Every full run now opens a row in pipeline_run and closes
it with how the run ended. Two rules are pinned here: writing the log can never fail or block a run,
and a dbt run that partly failed is recorded as such even though run_dbt still reports success.
"""
import importlib
import json
import subprocess

import pytest

import pipeline_runlog

DBT_PARTIAL = (
    "1 of 40 OK created sql table model public.dim_date ........ [SELECT 1 in 0.1s]\n"
    "3 of 40 ERROR creating sql table model public.agg_x ....... [ERROR in 0.5s]\n"
    "Done. PASS=38 WARN=0 ERROR=1 SKIP=1 TOTAL=40\n"
)


class Result:
    def __init__(self, value=None):
        self.value = value

    def scalar(self):
        return self.value


class Conn:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, params=None):
        self.engine.calls.append((str(statement), params or {}))
        return Result(self.engine.next_id)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Engine:
    """Records every statement; INSERT ... RETURNING yields next_id."""

    def __init__(self, next_id=7):
        self.calls = []
        self.next_id = next_id

    def begin(self):
        return Conn(self)


class BrokenEngine:
    def begin(self):
        raise RuntimeError("database is down")


def test_open_run_creates_table_and_returns_id():
    engine = Engine(next_id=7)
    assert pipeline_runlog.open_run(engine) == 7
    assert "CREATE TABLE IF NOT EXISTS pipeline_run" in engine.calls[0][0]
    insert_sql, params = engine.calls[-1]
    assert "INSERT INTO pipeline_run" in insert_sql
    assert params["status"] == "running"


def test_close_run_sets_status_stage_error_and_summary():
    engine = Engine()
    pipeline_runlog.close_run(engine, 7, "finished_with_errors", failed_stage="dbt", error="x" * 900,
                              summary={"failed_models": ["public.agg_x"], "kpi_stage_ran": False})
    sql, params = engine.calls[-1]
    assert "UPDATE pipeline_run" in sql
    assert params["run_id"] == 7
    assert params["status"] == "finished_with_errors"
    assert params["failed_stage"] == "dbt"
    assert len(params["error"]) == 500
    assert json.loads(params["summary"]) == {"failed_models": ["public.agg_x"], "kpi_stage_ran": False}


def test_update_summary_merges_a_patch():
    engine = Engine()
    pipeline_runlog.update_summary(engine, 7, {"record_published": True})
    sql, params = engine.calls[-1]
    assert "summary = summary ||" in sql
    assert json.loads(params["patch"]) == {"record_published": True}


def test_logging_never_raises():
    assert pipeline_runlog.open_run(BrokenEngine()) is None
    pipeline_runlog.close_run(Engine(), None, "failed")              # no run was opened: nothing to close
    pipeline_runlog.close_run(BrokenEngine(), 5, "failed")
    pipeline_runlog.update_summary(BrokenEngine(), 5, {"a": 1})
    pipeline_runlog.update_summary(Engine(), None, {"a": 1})


def test_close_run_without_a_run_writes_nothing():
    engine = Engine()
    pipeline_runlog.close_run(engine, None, "failed")
    assert engine.calls == []


def test_parse_dbt_outcome_names_failed_models():
    assert pipeline_runlog.parse_dbt_outcome(DBT_PARTIAL) == {"failed_models": ["public.agg_x"]}
    assert pipeline_runlog.parse_dbt_outcome(None) == {"failed_models": []}


# ---- wiring in ingest.py ----

@pytest.fixture
def ingest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)                 # importing ingest opens ingest.log in the working directory
    module = importlib.import_module("ingest")
    (tmp_path / "dbt_project.yml").write_text("name: x\n")
    (tmp_path / "profiles.yml").write_text("x: {}\n")
    monkeypatch.setattr(module, "get_project_root", lambda: tmp_path)
    monkeypatch.delenv("RENDER", raising=False)
    return module


def fake_dbt(fail_on=None, stdout="", seen=None):
    """subprocess.run stand-in: `fail_on` is the dbt run stage that raises ('marts', 'kpi' or 'record').
    `seen`, when given, collects the name of each dbt run stage in the order it was started."""
    def run(command, **kwargs):
        is_marts = "--exclude" in command
        is_kpi = "--select" in command and "kpi.*" in command
        is_record = "--select" in command and "tag:calculation_record" in command
        stage = "marts" if is_marts else "kpi" if is_kpi else "record" if is_record else None
        if seen is not None and stage:
            seen.append((stage, list(command)))
        if stage and stage == fail_on:
            raise subprocess.CalledProcessError(1, command, output=stdout, stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
    return run


def test_run_dbt_success_is_recorded(ingest, monkeypatch):
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())
    assert ingest.run_dbt() is True
    assert ingest.LAST_DBT_OUTCOME == {"ok": True, "partial": False, "failed_stage": None,
                                       "failed_models": [], "kpi_stage_ran": True, "error": None}


def test_run_dbt_partial_failure_is_recorded_but_still_returns_true(ingest, monkeypatch):
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(fail_on="marts", stdout=DBT_PARTIAL))
    assert ingest.run_dbt() is True              # unchanged behaviour: partial success counts as success
    outcome = ingest.LAST_DBT_OUTCOME
    assert outcome["ok"] is True and outcome["partial"] is True
    assert outcome["failed_stage"] == "dbt"
    assert outcome["failed_models"] == ["public.agg_x"]
    assert outcome["kpi_stage_ran"] is False     # stage 1 raised, so the kpi stage never started


def test_run_dbt_partial_failure_in_the_kpi_stage(ingest, monkeypatch):
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(fail_on="kpi", stdout=DBT_PARTIAL))
    assert ingest.run_dbt() is True
    assert ingest.LAST_DBT_OUTCOME["failed_stage"] == "dbt-kpi"
    assert ingest.LAST_DBT_OUTCOME["kpi_stage_ran"] is True


def test_run_dbt_hard_failure_returns_false(ingest, monkeypatch):
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(fail_on="marts", stdout="Compilation Error"))
    assert ingest.run_dbt() is False
    outcome = ingest.LAST_DBT_OUTCOME
    assert outcome["ok"] is False and outcome["partial"] is False
    assert outcome["failed_stage"] == "dbt"


class RunLog:
    """Stands in for pipeline_runlog inside ingest: records what main() reports."""

    parse_dbt_outcome = staticmethod(pipeline_runlog.parse_dbt_outcome)   # pure; the real one

    def __init__(self):
        self.opened = 0
        self.closed = []
        self.patches = []

    def open_run(self, engine):
        self.opened += 1
        return 42

    def close_run(self, engine, run_id, status, failed_stage=None, error=None, summary=None):
        self.closed.append({"run_id": run_id, "status": status, "failed_stage": failed_stage,
                            "error": error, "summary": summary})

    def update_summary(self, engine, run_id, patch):
        self.patches.append(patch)


@pytest.fixture
def main_env(ingest, monkeypatch):
    log = RunLog()
    monkeypatch.setattr(ingest, "pipeline_runlog", log)
    monkeypatch.setattr(ingest, "load_environment", lambda: None)
    monkeypatch.setattr(ingest, "create_database_engine", lambda: object())
    for name in ("C7_AUTH_TOKEN", "C7_TENANT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("X_TOCK_AUTH", "test")
    monkeypatch.setenv("X_TOCK_SCOPE", "test")
    monkeypatch.setattr(ingest.TockAPIClient, "get_data_urls", lambda self: {"guest_urls": [], "reservation_urls": []})
    monkeypatch.setattr(ingest.TockAPIClient, "get_watermark", lambda self, table: None)
    monkeypatch.setattr(ingest.TockAPIClient, "sync_export", lambda self, table, urls: 0)
    return ingest, log


def test_main_closes_run_as_succeeded(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())
    ingest.main()
    assert [c["status"] for c in log.closed] == ["succeeded"]
    assert log.closed[0]["run_id"] == 42
    assert log.closed[0]["summary"]["kpi_stage_ran"] is True


def test_main_closes_run_as_finished_with_errors_on_partial_dbt_failure(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(fail_on="marts", stdout=DBT_PARTIAL))
    ingest.main()                                 # exits 0 today, and still does
    assert log.closed[0]["status"] == "finished_with_errors"
    assert log.closed[0]["failed_stage"] == "dbt"
    assert log.closed[0]["summary"]["failed_models"] == ["public.agg_x"]
    assert log.closed[0]["summary"]["kpi_stage_ran"] is False


def test_main_closes_run_as_failed_when_dbt_fails(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(fail_on="kpi", stdout="Database Error"))
    with pytest.raises(SystemExit) as exit_info:
        ingest.main()
    assert exit_info.value.code == 1
    assert log.closed[0]["status"] == "failed"
    assert log.closed[0]["failed_stage"] == "dbt-kpi"


def test_main_closes_run_as_failed_when_ingest_raises(main_env, monkeypatch):
    ingest, log = main_env

    def boom(self):
        raise RuntimeError("Tock export unavailable")
    monkeypatch.setattr(ingest.TockAPIClient, "get_data_urls", boom)
    with pytest.raises(SystemExit) as exit_info:
        ingest.main()
    assert exit_info.value.code == 1
    assert len(log.closed) == 1
    assert log.closed[0]["status"] == "failed"
    assert log.closed[0]["failed_stage"] == "ingest"
    assert "Tock export unavailable" in log.closed[0]["error"]


def test_single_endpoint_run_is_not_logged(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())
    ingest.main("tock-guest")
    assert log.opened == 0
    assert [c["run_id"] for c in log.closed] == [None]      # close_run ignores a run that was never opened


def test_main_publishes_the_record_and_checks_the_walk(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())
    monkeypatch.setattr(ingest.record_publish, "publish", lambda engine, payload, git_sha=None: True)
    monkeypatch.setattr(ingest.record_walk, "check_walk",
                        lambda engine: {"ok": True, "mismatched_days": 0, "first_mismatch": None})
    ingest.main()
    assert log.patches == [{"record_published": True},
                           {"walk_check": {"ok": True, "mismatched_days": 0, "first_mismatch": None}}]
    assert log.closed[0]["status"] == "succeeded"


def test_main_survives_publish_failure(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())

    def boom(*args, **kwargs):
        raise RuntimeError("entries unreadable")
    monkeypatch.setattr(ingest.record_publish, "build_payload", boom)
    monkeypatch.setattr(ingest.record_walk, "check_walk", boom)
    ingest.main()                                  # no exception, no exit
    assert {"record_published": False} in log.patches
    assert {"walk_check": None} in log.patches
    assert log.closed[0]["status"] == "succeeded"


def test_single_endpoint_run_publishes_nothing(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())
    ingest.main("tock-guest")
    assert log.patches == []


def test_main_runs_when_the_record_modules_could_not_be_imported(main_env, monkeypatch):
    # ingest.py guards its import of the metrics package: if that import fails on a deploy, the two
    # names are None and the pipeline must still run.
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt())
    monkeypatch.setattr(ingest, "record_publish", None)
    monkeypatch.setattr(ingest, "record_walk", None)
    ingest.main()
    assert log.patches == [{"record_published": False}, {"walk_check": None}]
    assert log.closed[0]["status"] == "succeeded"


# ---- the record's own dbt model must not be able to hold back the tiles ----
# rec_tock_revenue_daily evaluates casts nothing else does. If it built in the marts stage, one
# malformed Tock value would make dbt exit 1, run_dbt would call that a partial success, and the kpi
# stage (the tiles) would be skipped on every run. So it is built on its own, after the tiles.

def test_marts_stage_does_not_build_the_record_models(ingest, monkeypatch):
    seen = []
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(seen=seen))
    assert ingest.run_dbt() is True
    stage, command = seen[0]
    assert stage == "marts"
    excluded = command[command.index("--exclude") + 1:]
    assert "kpi.*" in excluded and "tag:calculation_record" in excluded
    assert [name for name, _ in seen] == ["marts", "kpi"]      # run_dbt itself never builds them


def test_record_models_are_built_after_the_tiles(main_env, monkeypatch):
    ingest, log = main_env
    seen = []
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(seen=seen))
    ingest.main()
    assert [name for name, _ in seen] == ["marts", "kpi", "record"]
    assert log.closed[0]["status"] == "succeeded"
    assert log.closed[0]["summary"]["record_models_built"] is True


def test_a_failing_record_model_leaves_the_run_succeeded(main_env, monkeypatch):
    ingest, log = main_env
    monkeypatch.setattr(ingest.subprocess, "run", fake_dbt(fail_on="record", stdout=DBT_PARTIAL))
    ingest.main()                                  # no exit, no exception
    assert log.closed[0]["status"] == "succeeded"
    assert log.closed[0]["summary"]["kpi_stage_ran"] is True
    assert log.closed[0]["summary"]["record_models_built"] is False
    assert log.closed[0]["summary"]["failed_models"] == []     # the tiles' models did not fail
