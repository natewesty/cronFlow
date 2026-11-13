"""The Calculation Record's entries.

One YAML file per dashboard metric lives in metrics/entries, written in plain words. The record is
only worth having if it cannot drift from the code, so these tests pin two things: an entry is tied
to the model files it describes (and everything those ref), and a change to any of them fails the
suite until a person re-confirms the entry.

    .venv/bin/python -m metrics status          which entries are stale, and which files changed
    .venv/bin/python -m metrics confirm <id>    re-confirm after checking the entry against the code
"""
import json
from pathlib import Path

import pytest
import yaml

from metrics import record

REPO = Path(__file__).resolve().parents[1]

ENTRY = {
    "id": "tasting-fees",
    "name": "Tasting Fees",
    "kpi_id": 2,
    "group": "Tasting Room",
    "shown_on": "Sales Metrics & Tracking",
    "summary": "What guests paid.",
    "calculation": ["Take every reservation.", "Add up what was paid."],
    "includes": ["Sales tax"],
    "excludes": ["Cancelled reservations"],
    "counted_on": "The day of the visit.",
    "sources": ["Tock"],
    "history": [{"date": "2025-10-07", "change": "Changed to net paid.", "effect": "Lower."}],
    "models": ["models/a.sql"],
}


@pytest.fixture
def repo(tmp_path):
    """A small stand-in repository: a.sql refs b.sql, b.sql reads a source; one entry."""
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.sql").write_text("select * from {{ ref('b') }}\n")
    (tmp_path / "models" / "b.sql").write_text('select 1 from {{ source("raw", "c") }}\n')
    (tmp_path / "models" / "c.sql").write_text("select 2\n")
    (tmp_path / "metrics" / "entries").mkdir(parents=True)
    write_entry(tmp_path, ENTRY)
    (tmp_path / "metrics" / "events.yml").write_text("[]\n")
    (tmp_path / "metrics" / "confirmed.json").write_text("{}\n")
    return tmp_path


def write_entry(root, entry, name=None):
    path = root / "metrics" / "entries" / f"{name or entry['id']}.yml"
    path.write_text(yaml.safe_dump(entry, sort_keys=False))


# ---- which files an entry depends on ----

def test_closure_follows_ref_transitively(repo):
    assert record.model_closure(["models/a.sql"], repo) == ["models/a.sql", "models/b.sql"]


def test_closure_ignores_source(repo):
    assert "models/c.sql" not in record.model_closure(["models/b.sql"], repo)


def test_closure_follows_refs_made_inside_macros(repo):
    # agg_kpi_dashboard reaches kpi_dim_date only through a macro; the tile's periods depend on it.
    (repo / "macros").mkdir()
    (repo / "macros" / "dates.sql").write_text("{% macro bounds() %} select * from {{ ref('c') }} {% endmacro %}\n")
    assert record.model_closure(["models/b.sql"], repo) == ["models/b.sql", "models/c.sql"]


def test_fingerprint_covers_macros(repo):
    (repo / "macros").mkdir()
    (repo / "macros" / "fiscal.sql").write_text("{% macro start() %}7{% endmacro %}\n")
    record.confirm(["tasting-fees"], "2026-11-13", repo)
    (repo / "macros" / "fiscal.sql").write_text("{% macro start() %}1{% endmacro %}\n")
    state = record.staleness(record.load_entries(repo)[0], record.load_lock(repo), repo)
    assert state["fingerprint_ok"] is False
    assert state["changed_files"] == ["macros/fiscal.sql"]


# ---- what a valid entry is ----

def test_a_complete_entry_is_valid(repo):
    assert record.validate_entry(record.load_entries(repo)[0], repo) == []


def test_validate_rejects_unknown_field(repo):
    problems = record.validate_entry({**ENTRY, "known_issue": "x"}, repo)
    assert any("known_issue" in p for p in problems)


@pytest.mark.parametrize("field", record.REQUIRED)
def test_validate_requires_every_required_field(repo, field):
    entry = {k: v for k, v in ENTRY.items() if k != field}
    assert any(field in p for p in record.validate_entry(entry, repo))


def test_validate_rejects_missing_model_file(repo):
    problems = record.validate_entry({**ENTRY, "models": ["models/gone.sql"]}, repo)
    assert any("models/gone.sql" in p for p in problems)


def test_validate_id_must_match_filename(repo):
    write_entry(repo, ENTRY, name="other-name")
    loaded = [e for e in record.load_entries(repo) if e["_file"] == "other-name.yml"][0]
    assert any("file name" in p for p in record.validate_entry(loaded, repo))


def test_validate_rejects_a_group_outside_the_list(repo):
    assert any("group" in p for p in record.validate_entry({**ENTRY, "group": "Misc"}, repo))


def test_validate_checks_will_not_match_and_walk(repo):
    bad = {**ENTRY, "will_not_match": [{"figure": "Tock sales", "reason": "tax", "direction": "bigger"}],
           "walk": {"kind": "tock_revenue", "bucket": "Lounge"}}
    problems = record.validate_entry(bad, repo)
    assert any("direction" in p for p in problems)
    assert any("walk" in p for p in problems)
    good = {**ENTRY, "will_not_match": [{"figure": "Tock sales", "reason": "tax", "direction": "lower"}],
            "walk": {"kind": "tock_revenue", "bucket": "Tasting Room"}}
    assert record.validate_entry(good, repo) == []


def test_history_and_the_include_exclude_lists_may_be_empty(repo):
    # Most entries have no history of their own (pipeline-wide events are merged in when published),
    # and a simple count may leave nothing out.
    assert record.validate_entry({**ENTRY, "history": [], "includes": [], "excludes": []}, repo) == []


def test_validate_checks_history_dates(repo):
    bad = {**ENTRY, "history": [{"date": "last week", "change": "x", "effect": "y"}]}
    assert any("history" in p for p in record.validate_entry(bad, repo))


# ---- staleness ----

def test_staleness_unconfirmed_is_not_ok(repo):
    entry = record.load_entries(repo)[0]
    assert record.staleness(entry, record.load_lock(repo), repo) == {
        "confirmed_date": None, "fingerprint_ok": False, "changed_files": []}


def test_staleness_ok_after_confirm(repo):
    record.confirm(["tasting-fees"], "2026-11-13", repo)
    entry = record.load_entries(repo)[0]
    assert record.staleness(entry, record.load_lock(repo), repo) == {
        "confirmed_date": "2026-11-13", "fingerprint_ok": True, "changed_files": []}


def test_staleness_flags_upstream_change(repo):
    record.confirm(["tasting-fees"], "2026-11-13", repo)
    (repo / "models" / "b.sql").write_text("select 99\n")
    entry = record.load_entries(repo)[0]
    state = record.staleness(entry, record.load_lock(repo), repo)
    assert state["fingerprint_ok"] is False
    assert state["changed_files"] == ["models/b.sql"]
    assert state["confirmed_date"] == "2026-11-13"


def test_staleness_flags_a_newly_referenced_model(repo):
    record.confirm(["tasting-fees"], "2026-11-13", repo)
    (repo / "models" / "a.sql").write_text("select * from {{ ref('b') }} join {{ ref('c') }} using (x)\n")
    state = record.staleness(record.load_entries(repo)[0], record.load_lock(repo), repo)
    assert state["changed_files"] == ["models/a.sql", "models/c.sql"]


def test_confirm_rejects_an_unknown_entry(repo):
    with pytest.raises(KeyError):
        record.confirm(["no-such-entry"], "2026-11-13", repo)


def test_lock_is_written_as_sorted_json(repo):
    record.confirm(["tasting-fees"], "2026-11-13", repo)
    lock = json.loads((repo / "metrics" / "confirmed.json").read_text())
    assert list(lock["tasting-fees"]["files"]) == ["models/a.sql", "models/b.sql"]
    assert lock["tasting-fees"]["date"] == "2026-11-13"


# ---- the real repository: the living tests ----

def test_kpi_columns_reads_all_31():
    columns = record.kpi_columns(REPO)
    assert sorted(columns) == list(range(1, 32))
    assert columns[2] == "tasting_room_fees_revenue"
    assert columns[31] == "tasting_room_bottles_sold"


def test_every_kpi_has_exactly_one_entry():
    ids = [e["kpi_id"] for e in record.load_entries(REPO) if e.get("kpi_id") is not None]
    assert sorted(ids) == sorted(record.kpi_columns(REPO)), "every dashboard metric needs one entry in metrics/entries"


def test_every_entry_is_valid():
    problems = {e["id"]: record.validate_entry(e, REPO) for e in record.load_entries(REPO)}
    assert {k: v for k, v in problems.items() if v} == {}


def test_every_entry_is_confirmed_against_current_models():
    lock = record.load_lock(REPO)
    stale = {}
    for entry in record.load_entries(REPO):
        state = record.staleness(entry, lock, REPO)
        if not state["fingerprint_ok"]:
            stale[entry["id"]] = state["changed_files"] or "never confirmed"
    assert stale == {}, (
        "These entries describe models that have changed since they were last confirmed. Check each entry "
        "against the code, update it (and its history) if the calculation changed, then run: "
        ".venv/bin/python -m metrics confirm <id>"
    )


def test_events_name_real_entries():
    ids = {e["id"] for e in record.load_entries(REPO)}
    for event in record.load_events(REPO):
        assert set(event) == {"date", "change", "effect", "metrics"}
        if event["metrics"] != "all":
            assert set(event["metrics"]) <= ids, event


def _texts(entry):
    """Every sentence a reader of the entry sees."""
    out = [entry["summary"], entry["counted_on"], entry["shown_on"]]
    for field in ("calculation", "includes", "excludes"):
        out += entry.get(field) or []
    for item in entry.get("will_not_match") or []:
        out += [item["figure"], item["reason"]]
    for item in entry.get("history") or []:
        out += [item["change"], item["effect"]]
    return out


def test_every_kpi_entry_is_tied_to_the_roll_up_the_tiles_read():
    # The number on screen is agg_kpi_dashboard's period roll-up of the daily value, so a change to the
    # roll-up, the KPI-to-column mapping, the date dimension or the fiscal macros must flag the entry.
    needed = {"models/kpi/agg_kpi_dashboard.sql", "models/kpi/fact_kpi_daily.sql", "models/kpi/kpi_dim_date.sql",
              "models/marts/agg_daily_revenue.sql", "macros/fiscal.sql", "macros/kpi_date_utils.sql"}
    for entry in record.load_entries(REPO):
        if entry.get("kpi_id") is not None:
            missing = needed - set(record.file_hashes(entry["models"], REPO))
            assert not missing, (entry["id"], sorted(missing))


def test_entries_make_no_dated_claims_about_data():
    # "As of <date> no order carries ..." is a statement about data that nothing re-checks. An entry
    # describes what the code does; what the data happened to contain on one day does not belong in it.
    import re
    dated = re.compile(r"\bas of \d{4}-\d{2}-\d{2}\b", re.IGNORECASE)
    offenders = {e["id"]: [t for t in _texts(e) if dated.search(t)] for e in record.load_entries(REPO)}
    assert {k: v for k, v in offenders.items() if v} == {}


def test_daily_averages_are_described_as_daily():
    # These two are computed per day; the dashboard then averages the days. A summary that reads as a
    # per-order or per-guest average over the whole period would describe a different number.
    entries = {e["id"]: e for e in record.load_entries(REPO)}
    for entry_id in ("average-order-value", "avg-tasting-fee-per-guest"):
        assert "each day" in entries[entry_id]["summary"].lower(), entry_id


def test_history_records_the_changes_that_moved_values():
    events = {e["date"]: e for e in record.load_events(REPO)}
    excluded = events["2026-11-04"]                      # excluded customers' orders removed from reporting
    assert {"tasting-room-wine-revenue", "total-orders", "total-daily-revenue"} <= set(excluded["metrics"])
    tock = events["2026-11-10"]                          # the per-guest tile's guest count moved too
    assert "tasting-room-bottles-sold" in tock["metrics"]
    wine = {e["id"]: e for e in record.load_entries(REPO)}["tasting-room-wine-revenue"]
    assert "2025-10-20" in [item["date"] for item in wine["history"]]
