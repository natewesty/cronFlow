"""Load, validate and fingerprint the Calculation Record's entries."""
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]

REQUIRED = ("id", "name", "group", "shown_on", "summary", "calculation", "includes", "excludes",
            "counted_on", "sources", "history", "models")
OPTIONAL = ("kpi_id", "will_not_match", "walk")
GROUPS = ("Tasting Room", "Wine Club", "Channels", "Events", "Shipping and Total", "Traffic and Guests",
          "Wine Sales", "Membership", "Orders", "Reservations")
DIRECTIONS = ("higher", "lower", "differs")
WALK_BUCKETS = ("Tasting Room", "Club", "Event")
# A simple count may leave nothing out, and most entries have no history of their own (pipeline-wide
# events are merged in when the record is published). Every other field must have content.
MAY_BE_EMPTY = ("includes", "excludes", "history")

_ID = re.compile(r"^[a-z0-9-]+$")
_REF = re.compile(r"""ref\(\s*['"]([A-Za-z0-9_]+)['"]\s*\)""")
_KPI_ROW = re.compile(
    r"select\s+date_day(?:\s+as\s+date_key)?\s*,\s*(\d+)(?:\s+as\s+kpi_id)?\s*,\s*0(?:\s+as\s+entity_id)?\s*,\s*(\w+)",
    re.IGNORECASE)


def _root(root) -> Path:
    return Path(root) if root is not None else REPO_ROOT


def load_entries(root=None) -> List[dict]:
    """Every entry, sorted by id. `_file` is the file it came from (not part of the entry)."""
    entries = []
    for path in sorted((_root(root) / "metrics" / "entries").glob("*.yml")):
        entry = yaml.safe_load(path.read_text()) or {}
        entry["_file"] = path.name
        for item in entry.get("history") or []:
            if isinstance(item, dict) and "date" in item:
                item["date"] = _iso(item["date"])
        entries.append(entry)
    return sorted(entries, key=lambda e: str(e.get("id", "")))


def load_events(root=None) -> List[dict]:
    path = _root(root) / "metrics" / "events.yml"
    events = yaml.safe_load(path.read_text()) if path.exists() else []
    return [{**e, "date": _iso(e.get("date"))} for e in (events or [])]


def _iso(value) -> Optional[str]:
    """YAML reads an unquoted 2026-07-01 as a date; the record carries dates as text."""
    return value.isoformat() if isinstance(value, date) else value


def _is_iso_date(value) -> bool:
    try:
        date.fromisoformat(str(_iso(value)))
        return True
    except ValueError:
        return False


def validate_entry(entry: dict, root=None) -> List[str]:
    """Everything wrong with an entry; empty when it is valid."""
    root = _root(root)
    problems = []
    allowed = set(REQUIRED) | set(OPTIONAL) | {"_file"}
    for field in sorted(set(entry) - allowed):
        problems.append(f"unknown field: {field}")
    for field in REQUIRED:
        value = entry.get(field)
        if value is None or (value in ("", []) and field not in MAY_BE_EMPTY):
            problems.append(f"missing or empty: {field}")

    entry_id = str(entry.get("id", ""))
    if entry_id and not _ID.match(entry_id):
        problems.append("id must be lower-case letters, digits and hyphens")
    if entry.get("_file") and entry["_file"] != f"{entry_id}.yml":
        problems.append(f"id does not match its file name ({entry['_file']})")
    if entry.get("group") and entry["group"] not in GROUPS:
        problems.append(f"group must be one of {', '.join(GROUPS)}")
    for field in ("calculation", "includes", "excludes", "sources", "models"):
        if entry.get(field) is not None and not isinstance(entry[field], list):
            problems.append(f"{field} must be a list")
    if entry.get("kpi_id") is not None and not isinstance(entry["kpi_id"], int):
        problems.append("kpi_id must be a whole number")

    for model in entry.get("models") or []:
        if not (root / str(model)).is_file():
            problems.append(f"models names a file that does not exist: {model}")

    for item in entry.get("history") or []:
        if not isinstance(item, dict) or set(item) != {"date", "change", "effect"} or not _is_iso_date(item["date"]):
            problems.append(f"history items need date (YYYY-MM-DD), change and effect: {item}")
    for item in entry.get("will_not_match") or []:
        if not isinstance(item, dict) or set(item) != {"figure", "reason", "direction"}:
            problems.append(f"will_not_match items need figure, reason and direction: {item}")
        elif item["direction"] not in DIRECTIONS:
            problems.append(f"will_not_match direction must be one of {', '.join(DIRECTIONS)}: {item['direction']}")
    walk = entry.get("walk")
    if walk is not None and (not isinstance(walk, dict) or walk.get("kind") != "tock_revenue"
                             or walk.get("bucket") not in WALK_BUCKETS or set(walk) != {"kind", "bucket"}):
        problems.append(f"walk must be {{kind: tock_revenue, bucket: one of {', '.join(WALK_BUCKETS)}}}")
    return problems


def model_closure(models: List[str], root=None) -> List[str]:
    """The given model files plus every model they ref(), transitively. source() is not followed."""
    root = _root(root)
    by_name = {path.stem: path.relative_to(root).as_posix() for path in sorted((root / "models").rglob("*.sql"))}
    seen, queue = set(), [str(m) for m in models]
    # A model can reach another only through a macro (agg_kpi_dashboard reaches kpi_dim_date that way),
    # and macros are not tied to one model, so whatever any macro refs belongs to every closure.
    for macro in _macro_files(root):
        queue += [by_name[name] for name in _REF.findall((root / macro).read_text()) if name in by_name]
    while queue:
        rel = queue.pop()
        if rel in seen or not (root / rel).is_file():
            continue
        seen.add(rel)
        for name in _REF.findall((root / rel).read_text()):
            if name in by_name:
                queue.append(by_name[name])
    return sorted(seen)


def _macro_files(root: Path) -> List[str]:
    return sorted(path.relative_to(root).as_posix() for path in (root / "macros").rglob("*.sql"))


def _config_files(root: Path) -> List[str]:
    """Seeds and project config: data and settings that change what the dashboard shows without a model
    changing (the excluded-customer list, the history start, the fiscal fallback)."""
    files = sorted(path.relative_to(root).as_posix() for path in (root / "models" / "seeds").glob("*.csv"))
    return files + (["dbt_project.yml"] if (root / "dbt_project.yml").is_file() else [])


def file_hashes(models: List[str], root=None) -> Dict[str, str]:
    """Hash of every file an entry's value depends on: its models, everything they ref, every macro
    (the fiscal and period logic lives there), and the seeds and project config."""
    root = _root(root)
    files = model_closure(models, root) + _macro_files(root) + _config_files(root)
    return {rel: hashlib.sha256((root / rel).read_bytes()).hexdigest() for rel in sorted(set(files))}


def load_lock(root=None) -> dict:
    path = _root(root) / "metrics" / "confirmed.json"
    return json.loads(path.read_text()) if path.exists() else {}


def confirm(entry_ids: List[str], today: str, root=None) -> None:
    """Record that these entries were checked against the code as it is now."""
    root = _root(root)
    entries = {e["id"]: e for e in load_entries(root)}
    lock = load_lock(root)
    for entry_id in entry_ids:
        if entry_id not in entries:
            raise KeyError(entry_id)
        lock[entry_id] = {"date": today, "files": file_hashes(entries[entry_id].get("models") or [], root)}
    (root / "metrics" / "confirmed.json").write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")


def staleness(entry: dict, lock: dict, root=None) -> dict:
    """Whether the models an entry describes are as they were when it was last confirmed."""
    held = lock.get(entry.get("id"))
    if not held:
        return {"confirmed_date": None, "fingerprint_ok": False, "changed_files": []}
    now = file_hashes(entry.get("models") or [], root)
    then = held.get("files", {})
    changed = sorted(path for path in set(now) | set(then) if now.get(path) != then.get(path))
    return {"confirmed_date": held.get("date"), "fingerprint_ok": not changed, "changed_files": changed}


def kpi_columns(root=None) -> Dict[int, str]:
    """kpi_id -> agg_daily_revenue column, as fact_kpi_daily unpivots them."""
    sql = (_root(root) / "models" / "kpi" / "fact_kpi_daily.sql").read_text()
    return {int(kpi_id): column for kpi_id, column in _KPI_ROW.findall(sql)}
