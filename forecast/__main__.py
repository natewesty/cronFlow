"""CLI: python -m forecast {run|backtest|schema}."""
from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

from . import io as fio
from .run import run


def ensure_schema_only(url: Optional[str] = None) -> None:
    engine = fio.get_engine(url)
    with engine.begin() as conn:
        fio.ensure_schema(conn)


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv("cronDon.env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        stream=sys.stdout)
    parser = argparse.ArgumentParser(prog="forecast")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("run", "backtest"):
        sp = sub.add_parser(name)
        sp.add_argument("--origin", type=lambda s: pd.Timestamp(s), default=None)
        sp.add_argument("--horizon", type=int, default=None)
        sp.add_argument("--dry-run", action="store_true")
        sp.add_argument("--skip-backtest", action="store_true")
    sub.add_parser("schema")
    args = parser.parse_args(argv)
    try:
        if args.cmd == "schema":
            ensure_schema_only()
        elif args.cmd == "backtest":
            run(origin=args.origin, horizon=args.horizon or 1, dry_run=True, skip_backtest=False)
        else:
            run(origin=args.origin, horizon=args.horizon, dry_run=args.dry_run,
                skip_backtest=args.skip_backtest)
    except Exception:  # noqa: BLE001
        logging.getLogger("forecast").exception("forecast %s failed", args.cmd)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
