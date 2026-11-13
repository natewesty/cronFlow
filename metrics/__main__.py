"""python -m metrics status | confirm <id>... | confirm --all"""
import sys
from datetime import date

from metrics import record


def status() -> int:
    lock = record.load_lock()
    stale = 0
    for entry in record.load_entries():
        state = record.staleness(entry, lock)
        if state["fingerprint_ok"]:
            continue
        stale += 1
        if state["confirmed_date"] is None:
            print(f"{entry['id']}: never confirmed")
        else:
            print(f"{entry['id']}: changed since {state['confirmed_date']}")
            for path in state["changed_files"]:
                print(f"    {path}")
    print(f"{stale} stale" if stale else "every entry is confirmed against the current models")
    return 1 if stale else 0


def main(argv) -> int:
    if argv[:1] == ["status"]:
        return status()
    if argv[:1] == ["confirm"] and argv[1:]:
        ids = [e["id"] for e in record.load_entries()] if argv[1:] == ["--all"] else argv[1:]
        record.confirm(ids, date.today().isoformat())
        print(f"confirmed {len(ids)} entr{'y' if len(ids) == 1 else 'ies'}")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
