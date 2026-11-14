"""Publish the Calculation Record to the warehouse, where the dashboard reads it.

Runs at the start of every full pipeline run, before dbt, so the record is present even when dbt
fails. It replaces what was there in one transaction. Publishing must never fail or block a run:
publish() catches everything and returns False.

    public.calculation_record        one row per entry (entry_id, entry jsonb, published_at)
    public.calculation_record_meta   one row: events, the cron schedule, the deployed commit
"""
import json
import logging
from pathlib import Path
from typing import Optional

import yaml
from sqlalchemy import text

from metrics import record

logger = logging.getLogger(__name__)

CRON_SERVICE = "cronDon"

# The fields of a published entry, in order. `models` and the lock's file hashes stay in the repository.
PUBLISHED = ("id", "name", "kpi_id", "group", "shown_on", "summary", "calculation", "includes", "excludes",
             "counted_on", "sources", "will_not_match", "history", "walk")
DEFAULTS = {"kpi_id": None, "will_not_match": [], "history": [], "walk": None}

CREATE_TABLES = (
    """
    CREATE TABLE IF NOT EXISTS calculation_record (
        entry_id     text PRIMARY KEY,
        entry        jsonb NOT NULL,
        published_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS calculation_record_meta (
        id           int PRIMARY KEY CHECK (id = 1),
        published_at timestamptz NOT NULL,
        events       jsonb NOT NULL,
        schedule     jsonb NOT NULL,
        git_sha      text
    )
    """,
)


def read_schedule(root=None) -> Optional[dict]:
    """The ingest cron's schedule from render.yaml (a hand-kept mirror of the live Render service)."""
    path = (Path(root) if root is not None else record.REPO_ROOT) / "render.yaml"
    try:
        services = (yaml.safe_load(path.read_text()) or {}).get("services") or []
    except (OSError, yaml.YAMLError):
        return None
    for service in services:
        if service.get("name") == CRON_SERVICE and service.get("schedule"):
            return {"cron": str(service["schedule"]), "timezone": "UTC"}
    return None


def build_payload(root=None) -> dict:
    """Everything that gets published: entries with their staleness, events, schedule."""
    lock = record.load_lock(root)
    entries = []
    for entry in record.load_entries(root):
        published = {field: entry.get(field, DEFAULTS.get(field)) for field in PUBLISHED}
        published.update(record.staleness(entry, lock, root))
        entries.append(published)
    return {"entries": entries, "events": record.load_events(root), "schedule": read_schedule(root)}


def publish(engine, payload: dict, git_sha: Optional[str] = None) -> bool:
    """Replace the published record. True when it was written."""
    try:
        with engine.begin() as conn:
            for statement in CREATE_TABLES:
                conn.execute(text(statement))
            conn.execute(text("DELETE FROM calculation_record"))
            for entry in payload["entries"]:
                conn.execute(
                    text("INSERT INTO calculation_record (entry_id, entry, published_at) "
                         "VALUES (:entry_id, CAST(:entry AS jsonb), now())"),
                    {"entry_id": entry["id"], "entry": json.dumps(entry)},
                )
            conn.execute(
                text("INSERT INTO calculation_record_meta (id, published_at, events, schedule, git_sha) "
                     "VALUES (1, now(), CAST(:events AS jsonb), CAST(:schedule AS jsonb), :git_sha) "
                     "ON CONFLICT (id) DO UPDATE SET published_at = EXCLUDED.published_at, events = EXCLUDED.events, "
                     "schedule = EXCLUDED.schedule, git_sha = EXCLUDED.git_sha"),
                {"events": json.dumps(payload["events"]), "schedule": json.dumps(payload["schedule"]),
                 "git_sha": git_sha},
            )
        return True
    except Exception as error:
        logger.warning("Calculation record: could not publish (%s); the run continues", error)
        return False
