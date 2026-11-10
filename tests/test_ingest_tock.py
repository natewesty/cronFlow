"""Tock export sync.

Tock's nightly export is a full snapshot split across numbered files in reservation-date order;
every record carries the snapshot's stamp in versionId. When the snapshot shrinks to fewer files,
the old highest-numbered file is left behind, frozen. These tests pin the two rules that follow:
every file is read, and an older snapshot's copy never replaces a newer one.
"""
import importlib

import pytest

SEPT, NOV = 1789978036711, 1794348017694        # versionId of the Sept 21 and Nov 10 2026 snapshots


def reservation(rid, state, version, paid_cents=0):
    return {"id": rid, "partyState": state, "versionId": version, "netAmountPaidCents": paid_cents,
            "dateTime": "2026-10-03T11:00", "isCancelled": state == "CANCELLED", "payment": [], "refund": []}


@pytest.fixture
def ingest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)                 # importing ingest opens ingest.log in the working directory
    return importlib.import_module("ingest")


class Warehouse:
    """Stands in for the raw table: upserts replace by id, as the real ON CONFLICT (id) DO UPDATE does."""

    def __init__(self, rows=()):
        self.rows = {str(r["id"]): r for r in rows}
        self.writes = []

    def held_versions(self, table):
        return {rid: r.get("versionId") or 0 for rid, r in self.rows.items()}

    def upsert(self, table, records):
        self.writes.append([r["id"] for r in records])
        self.rows.update({str(r["id"]): r for r in records})


def sync(ingest, monkeypatch, files, warehouse):
    """Run the real sync over fake export files (url -> records) and a fake warehouse."""
    monkeypatch.setenv("X_TOCK_AUTH", "test")
    monkeypatch.setenv("X_TOCK_SCOPE", "test")
    client = ingest.TockAPIClient()
    client.fetch_json_data = lambda url: files[url]
    client.get_held_versions = warehouse.held_versions
    client.upsert_data = warehouse.upsert
    client.sync_export("tock_reservation", list(files))
    return warehouse


def test_update_in_a_lower_numbered_file_reaches_the_warehouse(ingest, monkeypatch):
    # The reported bug: the warehouse holds the Sept copy; the live copy is in file 1, the frozen one in file 10.
    warehouse = Warehouse([reservation(7, "EXPECTED", SEPT, 20000)])
    files = {
        "https://tock/reservation-1.json": [reservation(7, "LEFT", NOV, 24000), reservation(8, "EXPECTED", NOV, 9000)],
        "https://tock/reservation-10.json": [reservation(7, "EXPECTED", SEPT, 20000)],
    }

    sync(ingest, monkeypatch, files, warehouse)

    assert warehouse.rows["7"]["partyState"] == "LEFT"
    assert warehouse.rows["7"]["netAmountPaidCents"] == 24000
    assert warehouse.rows["8"]["netAmountPaidCents"] == 9000


@pytest.mark.parametrize("order", [["fresh", "frozen"], ["frozen", "fresh"]])
def test_frozen_file_never_wins_whatever_order_the_files_arrive_in(ingest, monkeypatch, order):
    contents = {
        "fresh": [reservation(7, "CANCELLED", NOV, 0)],
        "frozen": [reservation(7, "EXPECTED", SEPT, 20000)],
    }
    files = {f"https://tock/{name}.json": contents[name] for name in order}

    warehouse = sync(ingest, monkeypatch, files, Warehouse())

    assert warehouse.rows["7"]["partyState"] == "CANCELLED"
    assert warehouse.rows["7"]["netAmountPaidCents"] == 0


def test_older_copy_does_not_replace_a_newer_one_already_held(ingest, monkeypatch):
    # The live file failed to download this run (fetch gives []); the frozen file must not roll the row back.
    warehouse = Warehouse([reservation(7, "LEFT", NOV, 24000)])
    files = {
        "https://tock/reservation-9.json": [],
        "https://tock/reservation-10.json": [reservation(7, "EXPECTED", SEPT, 20000)],
    }

    sync(ingest, monkeypatch, files, warehouse)

    assert warehouse.rows["7"]["partyState"] == "LEFT"
    assert warehouse.writes == []


def test_second_run_on_the_same_snapshot_writes_nothing(ingest, monkeypatch):
    held = [reservation(7, "LEFT", NOV, 24000), reservation(8, "EXPECTED", NOV, 9000)]
    files = {"https://tock/reservation-1.json": [reservation(7, "LEFT", NOV, 24000), reservation(8, "EXPECTED", NOV, 9000)]}

    warehouse = sync(ingest, monkeypatch, files, Warehouse(held))

    assert warehouse.writes == []


def test_records_without_a_version_stamp_are_always_written(ingest, monkeypatch):
    # Older guest records carry no versionId; there is nothing to compare, so they are refreshed every run.
    unstamped_old = {"id": 5, "updatedAtTimestamp": 1700000000000, "nickName": "old"}
    unstamped_new = {"id": 5, "updatedAtTimestamp": 1790000000000, "nickName": "new"}

    warehouse = sync(ingest, monkeypatch, {"https://tock/guest-profile-1.json": [unstamped_new]}, Warehouse([unstamped_old]))

    assert warehouse.rows["5"]["nickName"] == "new"
