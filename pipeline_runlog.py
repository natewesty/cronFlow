"""Run log: one row in public.pipeline_run per full pipeline run.

The dashboard's Calculation Record page reads this table to say when the data last refreshed and
whether that run succeeded. Writing the log must never fail or block a run, so every function here
catches everything, logs it, and returns.

Statuses: running, succeeded, finished_with_errors, failed.
  finished_with_errors is the run that exits 0 although dbt reported failed models: ingest.run_dbt
  treats "some models failed, some passed" as success. After such a failure in the marts stage it
  still runs the kpi stage, so the tiles rebuild; summary.kpi_stage_ran says whether it did.
Stages (failed_stage): ingest, dbt, dbt-kpi.
"""
import json
import logging
import re
from typing import Dict, List, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

STATUSES = ("running", "succeeded", "finished_with_errors", "failed")
ERROR_LIMIT = 500

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id       bigserial PRIMARY KEY,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz,
    status       text NOT NULL,
    failed_stage text,
    error        text,
    summary      jsonb NOT NULL DEFAULT '{}'::jsonb
)
"""

FAILED_MODEL = re.compile(r"ERROR creating sql \w+ model (\S+)")


def open_run(engine) -> Optional[int]:
    """Record that a run has started. Returns its id, or None if the log could not be written."""
    try:
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLE))
            return conn.execute(
                text("INSERT INTO pipeline_run (started_at, status) VALUES (now(), :status) RETURNING run_id"),
                {"status": "running"},
            ).scalar()
    except Exception as error:
        logger.warning("Run log: could not open a run (%s); the run continues unlogged", error)
        return None


def close_run(engine, run_id: Optional[int], status: str, failed_stage: Optional[str] = None,
              error: Optional[str] = None, summary: Optional[Dict] = None) -> None:
    """Record how a run ended. `summary` keys are merged into what update_summary already wrote."""
    if run_id is None:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE pipeline_run SET finished_at = now(), status = :status, failed_stage = :failed_stage, "
                    "error = :error, summary = summary || CAST(:summary AS jsonb) WHERE run_id = :run_id"
                ),
                {"run_id": run_id, "status": status, "failed_stage": failed_stage,
                 "error": (error or None) and str(error)[:ERROR_LIMIT], "summary": json.dumps(summary or {})},
            )
    except Exception as failure:
        logger.warning("Run log: could not close run %s as %s (%s)", run_id, status, failure)


def update_summary(engine, run_id: Optional[int], patch: Dict) -> None:
    """Merge keys into a run's summary while it is still running."""
    if run_id is None:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE pipeline_run SET summary = summary || CAST(:patch AS jsonb) WHERE run_id = :run_id"),
                {"run_id": run_id, "patch": json.dumps(patch)},
            )
    except Exception as failure:
        logger.warning("Run log: could not update run %s (%s)", run_id, failure)


def parse_dbt_outcome(stdout: Optional[str]) -> Dict[str, List[str]]:
    """The models dbt reported as failed, read from its console output."""
    return {"failed_models": FAILED_MODEL.findall(stdout or "")}
