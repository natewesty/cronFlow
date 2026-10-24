# Forecast engine

Daily forecasting for the Donum KPI dashboard. Spec: `shad_dash_don/shad-dash-don/docs/superpowers/specs/2026-10-24-forecasting-pipeline-design.md`.

## Run

```bash
.venv/bin/python -m forecast run --dry-run        # compute, print summary, write nothing
.venv/bin/python -m forecast run                  # full run → forecast.* tables
.venv/bin/python -m forecast run --skip-backtest  # faster, no p10/p90 bands
.venv/bin/python -m forecast schema               # create the forecast schema/tables only
```

`--origin` backfills become the latest run and do not write a bookings snapshot; origins after yesterday (Pacific) are rejected.

A `--skip-backtest` run also becomes the latest run, so the app shows no p10/p90 bands and an empty Accuracy tab until the next full run. Against production (Oct 2026: 13 releases, ~3,600 members) a full run takes ~7 s: ~4 s compute including the backtest, ~1.5 s to write 4,960 rows. Sanity check before a first write: next-28-day total revenue vs the last 28 days is skewed by whether a club release falls in either window (3.4× on 2026-10-23 with the Nov 18–19 release ahead; 1.10× against the same 28 days last year).

Connection comes from `DATABASE_URL` (or `DB_*`), loaded from `cronDon.env` locally. `KPI_SCHEMA` overrides the source schema (default `public_nate_sandbox`).

## Tables (schema `forecast`)

`forecast_runs` (one row per run, `is_latest` marks the live one), `forecast_daily` (120 days × 31 KPIs per run), `forecast_monthly` (36 months of components), `forecast_accuracy` (rolling backtest per KPI and horizon bucket), `assumptions` (written by the app: `shipment_schedule`, `events`, `growth_overrides`, `engine_params`), `bookings_snapshot` (daily Tock on-the-books snapshot).

## Tests

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

## Deploy

`render.yaml` defines the `cronDon-forecast` cron (daily 13:30 UTC). After changing `render.yaml`, sync the blueprint in the Render dashboard and set the env vars.
