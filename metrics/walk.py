"""The run-time check that the Tock walk still ends at the dashboard's value.

dbt tests do not run in production, so after dbt the pipeline makes the comparison in
tests/assert_tock_walk_ties.sql itself and writes the answer into the run log. A mismatch never fails
a run; the dashboard's Calculation Record page shows it.
"""
import logging
import re
from pathlib import Path
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

TEST_FILE = Path(__file__).resolve().parents[1] / "tests" / "assert_tock_walk_ties.sql"


def tie_sql() -> str:
    """The dbt test, with each ref() replaced by the table it builds: one definition of "ties"."""
    return re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", TEST_FILE.read_text())


def check_walk(engine) -> Optional[dict]:
    """{"ok", "mismatched_days", "first_mismatch"}, or None if the comparison could not be made."""
    try:
        sql = tie_sql()
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
        days = sorted(str(row[0]) for row in rows)
        return {"ok": not days, "mismatched_days": len(days), "first_mismatch": days[0] if days else None}
    except Exception as error:
        logger.warning("Tock walk check could not run (%s)", error)
        return None
