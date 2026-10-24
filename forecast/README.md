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
