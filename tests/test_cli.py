import pandas as pd

from forecast import __main__ as cli
from forecast.run import RunSummary


def test_run_subcommand_forwards_args(monkeypatch):
    calls = {}

    def fake_run(origin=None, horizon=None, dry_run=False, skip_backtest=False, url=None):
        calls.update(origin=origin, horizon=horizon, dry_run=dry_run, skip_backtest=skip_backtest)
        return RunSummary(None, pd.Timestamp("2026-10-23"), 10, 31, 310, 1116, 0, 1.0)

    monkeypatch.setattr(cli, "run", fake_run)
    assert cli.main(["run", "--origin", "2026-10-23", "--horizon", "10", "--dry-run", "--skip-backtest"]) == 0
    assert calls == {"origin": pd.Timestamp("2026-10-23"), "horizon": 10, "dry_run": True, "skip_backtest": True}


def test_schema_subcommand(monkeypatch):
    done = []
    monkeypatch.setattr(cli, "ensure_schema_only", lambda url=None: done.append(True))
    assert cli.main(["schema"]) == 0 and done == [True]


def test_run_failure_returns_nonzero(monkeypatch):
    def fake_run(**kwargs):
        raise RuntimeError("boom")
    monkeypatch.setattr(cli, "run", fake_run)
    assert cli.main(["run"]) == 1
