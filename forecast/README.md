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

`forecast_runs` (one row per run, `is_latest` marks the live one), `forecast_daily` (120 days × 31 KPIs per run), `forecast_monthly` (36 months of components), `forecast_accuracy` (rolling backtest per KPI and horizon bucket), `assumptions` (written by the app: `shipment_schedule`, `events`, `growth_overrides`, `engine_params`), `bookings_snapshot` (daily Tock on-the-books snapshot), `forecast_models` (v3: one champion row per selectable KPI per run, with the full candidate grid as JSON).

## v2 release model

- Each scheduled club release is matched to a past release within ±30 days of the same date, up to 2 years back, preferring the same season; its size is scaled by membership then vs now.
- With no match, the pooled mean of the last four releases is the fallback.
- A saved club plan (tables `club_release_plan` and `club_release_tier_plan`) replaces the KPI 4 release size and scales 13/22/28 by the same ratio, in both the daily and the monthly path. `forecast_monthly.release_planned` marks the months it affects.
- Wine Club Fees (KPI 5) are placed at the matched release's fee offsets.
- The run's params JSON records `season` and `planned_revenue` (null when unplanned) for each scheduled release.

## v2 tables and run order

Run-scoped (keyed by `run_id`, pruned with the run): `forecast_backtest_periods` and `forecast_accuracy_periods` (week/month period backtest per KPI and periods ahead), `bottle_forecast` (monthly p50/p10/p90 per label, vintage and channel), `bottle_vintage_summary`, `bottle_profiles`, `bottle_backtest` (first-year retrospective per vintage). These six detail tables are also pruned early: rows of non-latest runs started more than 14 days ago are deleted on every successful run (`prune_runs(keep_days_detail=14)`); the run row and the v1 tables keep the 400-day rule. `forecast_runs.bottle_rows` records the `bottle_forecast` rows written. Not run-scoped: `club_release_history` (rebuilt on live-origin runs that produce history; backfills, dry runs and empty results leave it untouched). App-written plans: `label_plan`, `bottle_vintage_plan`, `club_release_plan`, `club_release_tier_plan`.

Run order (v3 inserts selection before `start_run`; see "v3 champion models"): load KPI history, OTB, members, assumptions, stored booking snapshots → load plans (club plan for this year and next, tier shares, label and vintage plans), bottle history and memberships → detect releases → champion selection (two backtests: year-ago model and recent-level model) → daily forecast on the champions → daily backtest on the champions (kept as a frame) → bands → monthly → period backtest (from the same frame; empty with `--skip-backtest`) → bottles (vintage table, price bands, vintage backtest, profiles, vintage forecast; skipped with a warning when there is no bottle history) → club history (orders for each release in the last 3 years, one window query per release; skipped with a warning when there are no memberships) → write everything in one transaction. `--dry-run` computes all of it and logs the counts (`periods=… bottles=… club_history=…`) and the month ahead=1 WMAPE for KPIs 4, 6 and 14.

Period p10/p90 bands are in-sample: the ratio percentiles come from the same backtest periods they are applied to.

Each step logs its wall time (`step=load|daily|backtest|monthly|periods|bottles|clubs|write <seconds>s`); the club step also logs `club tiers=… active_in_listed_tiers=… members=…` to compare listed-tier actives with `members_a0`.

Timings (2026-10-27 production dry run, origin 2026-10-26): load 4.8–5.5 s, daily 0.06 s, backtest 2.3–2.8 s, monthly 0.15 s, periods 0.5 s, bottles 0.35 s, clubs 4.4–4.7 s (12 window queries); total ≈ 13–14 s. Output: 7,068 period rows, ≈ 12,300 bottle rows, 230 club-history rows, 9 tiers.

Acceptance (2026-10-27): month ahead=1 WMAPE kpi 4 = 0.085, kpi 6 = 0.088 (target ≤ 0.15), kpi 14 = 0.114 (previous engine 0.185). Daily 8–28 WMAPE (previous → v2): kpi 4 0.316 → 0.148, kpi 6 0.348 → 0.165, kpi 13 0.92 → 0.53, kpi 14 0.413 → 0.332, kpi 5 1.82 → 1.18.

Bottle release rule: a vintage releases in its first month with ≥ 50 bottles. A vintage that never reached 50 releases on its first sale when no vintage of its label ever did (small labels, magnums) or when it is older than the label's newest vintage that did (library tail); otherwise it is unreleased (stray pre-release bottles): no release month, forecast as an incoming vintage whose lifetime already includes the bottles sold, and the default next vintage counts from the newest released one.

## v3 champion models

Each selectable KPI (`SELECTABLE_KPIS`: 1, 2, 4, 7, 8, 13, 15, 16, 18, 22, 28, 29, 30, 31) gets a champion: organic = `m x (w x year-ago model + (1 - w) x recent-level model)`.

- Candidates: the year-ago seasonal-weighted model (`swm`, the default) and a recent-level model scaled by a seasonal index.
- Grid: `w` in `champion_weights` (default 0, 0.25, 0.5, 0.75, 1; 1.0 is always present), each uncalibrated and with a leave-one-out bias calibration `m` (shrunk by `calibration_shrink`, skipped when the live ratio is off by more than `calibration_max`).
- Selection: two rolling backtests (year-ago and level), month-ahead=1 points per KPI, wMAPE per grid cell. The default is replaced only by a challenger at least `champion_margin` (0.005) better with at least `champion_min_months` (6) months. Ties go to the cell nearest the default (higher `w`, uncalibrated first). Months where the actual and both forecasts are zero are dropped after the two frames are joined.
- Final backtest: rerun with the champions in place; its month-ahead=1 wMAPE fills `forecast_models.wmape` (`wmape_default` is the year-ago model on the selection backtest). The daily bands and the period tables come from this backtest.
- Calendar flag: backtest origins that have a stored booking snapshot use it as on-the-books and are flagged `calendar` in `forecast_backtest_periods`.
- `forecast_models` holds one row per selectable KPI per run (`model`, `w_swm`, `calibration`, `wmape`, `wmape_default`, `wmape_selection`, `n`, `candidates`); the champions are also recorded in `forecast_runs.params` under `champions`.
- The monthly path mirrors the champion for future months (the current month keeps the daily path).
- `--skip-backtest` selects nothing: every KPI stays on the default model and no `forecast_models` rows are written. `--dry-run` logs one `champion kpi=... model=... w=... cal=... wmape=... default=...` line per changed KPI and `champions: X of Y KPIs changed`.
- Step order adds `select` after `load`.

KPI 28 "Total Orders" and KPI 29 "Average Order Value" come from `daily_order_metrics` in `models/kpi/fact_kpi_daily.sql`, which excludes tasting-lounge orders, event orders and external vendors. Since October 2025 the daily count is lower than Tasting Room Orders (KPI 30) on most days, which is impossible for a true total: 28 is effectively web, phone and club orders. The engine treats it as a small channel; the definition is left to the user (rename, or widen the filter in dbt).

Timings (2026-10-27 production dry run, origin 2026-10-26): load 5.2 s, select 5.2 s (two selection backtests + grid), daily 0.05 s, backtest 2.4 s (final, on champions), monthly 0.2 s, periods 0.5 s, bottles 0.4 s, clubs 4.8 s; total ≈ 19 s. Champions changed on 8 of 14 selectable KPIs (2, 4, 22, 30 blends; 8 blend 0.25; 28 level; 7 and 13 calibrated swm).

Acceptance (2026-10-27, final-backtest month-ahead wMAPE, v2 run 7 → v3): KPI 14 0.114 → 0.111, KPI 3 0.118 → 0.112, KPI 4 0.085 → 0.080, KPI 6 0.088 → 0.083, KPI 2 0.163 → 0.139, KPI 8 0.497 → 0.316, KPI 22 0.083 → 0.073, KPI 28 0.192 → 0.135, KPI 30 0.086 → 0.072; no selectable KPI worse than run 7; event KPIs unchanged. Known side effect: derived ratio KPIs 20 (0.131 → 0.158) and 21 move with KPI 2's champion; the margin rule guards selectable KPIs only.

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
