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

## v2 release model

- Each scheduled club release is matched to a past release within ±30 days of the same date, up to 2 years back, preferring the same season; its size is scaled by membership then vs now.
- With no match, the pooled mean of the last four releases is the fallback.
- A saved club plan (tables `club_release_plan` and `club_release_tier_plan`) replaces the KPI 4 release size and scales 13/22/28 by the same ratio, in both the daily and the monthly path. `forecast_monthly.release_planned` marks the months it affects.
- Wine Club Fees (KPI 5) are placed at the matched release's fee offsets.
- The run's params JSON records `season` and `planned_revenue` (null when unplanned) for each scheduled release.

## v2 tables and run order

Run-scoped (keyed by `run_id`, pruned with the run): `forecast_backtest_periods` and `forecast_accuracy_periods` (week/month period backtest per KPI and periods ahead), `bottle_forecast` (monthly p50/p10/p90 per label, vintage and channel), `bottle_vintage_summary`, `bottle_profiles`, `bottle_backtest` (first-year retrospective per vintage). These six detail tables are also pruned early: rows of non-latest runs started more than 14 days ago are deleted on every successful run (`prune_runs(keep_days_detail=14)`); the run row and the v1 tables keep the 400-day rule. `forecast_runs.bottle_rows` records the `bottle_forecast` rows written. Not run-scoped: `club_release_history` (rebuilt on live-origin runs that produce history; backfills, dry runs and empty results leave it untouched). App-written plans: `label_plan`, `bottle_vintage_plan`, `club_release_plan`, `club_release_tier_plan`.

Run order: load KPI history, OTB, members, assumptions → load plans (club plan for this year and next, tier shares, label and vintage plans), bottle history and memberships → detect releases → daily forecast → daily backtest (kept as a frame) → bands → monthly → period backtest (from the same frame; empty with `--skip-backtest`) → bottles (vintage table, price bands, vintage backtest, profiles, vintage forecast; skipped with a warning when there is no bottle history) → club history (orders for each release in the last 3 years, one window query per release; skipped with a warning when there are no memberships) → write everything in one transaction. `--dry-run` computes all of it and logs the counts (`periods=… bottles=… club_history=…`) and the month ahead=1 WMAPE for KPIs 4, 6 and 14.

Period p10/p90 bands are in-sample: the ratio percentiles come from the same backtest periods they are applied to.

Each step logs its wall time (`step=load|daily|backtest|monthly|periods|bottles|clubs|write <seconds>s`); the club step also logs `club tiers=… active_in_listed_tiers=… members=…` to compare listed-tier actives with `members_a0`. Timings with the v2 steps: _to be measured on the first production dry run_.

Timings (2026-10-27 production dry run, origin 2026-10-26): load 4.8–5.5 s, daily 0.06 s, backtest 2.3–2.8 s, monthly 0.15 s, periods 0.5 s, bottles 0.35 s, clubs 4.4–4.7 s (12 window queries); total ≈ 13–14 s. Output: 7,068 period rows, ≈ 12,300 bottle rows, 230 club-history rows, 9 tiers.

Acceptance (2026-10-27): month ahead=1 WMAPE kpi 4 = 0.085, kpi 6 = 0.088 (target ≤ 0.15), kpi 14 = 0.114 (previous engine 0.185). Daily 8–28 WMAPE (previous → v2): kpi 4 0.316 → 0.148, kpi 6 0.348 → 0.165, kpi 13 0.92 → 0.53, kpi 14 0.413 → 0.332, kpi 5 1.82 → 1.18.

Bottle release rule: a vintage releases in its first month with ≥ 50 bottles. A vintage that never reached 50 releases on its first sale when no vintage of its label ever did (small labels, magnums) or when it is older than the label's newest vintage that did (library tail); otherwise it is unreleased (stray pre-release bottles): no release month, forecast as an incoming vintage whose lifetime already includes the bottles sold, and the default next vintage counts from the newest released one.

## Known limitations

- Period p10/p90 bands are in-sample: the ratio percentiles come from the same backtest periods they are applied to.
- An assumed schedule date more than 7 days from an actual release double-counts fees (user-input error).
- One-off products (verticals, trios, etches) and large formats without a base-label profile get no bottle forecast and log "no profile … skipped".
- An under-threshold newest vintage stays unreleased until it sells ≥ 50 in a month or in ≥ 3 months.
- Detail tables of non-latest runs are pruned after 14 days.

## Tests

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

## Deploy

The `cronDon-forecast` cron runs on Render daily at 14:00 UTC (created by hand, mirrored in `render.yaml` for reference; there is no Blueprint). It carries the same database env vars as `cronDon` plus `KPI_SCHEMA`.
