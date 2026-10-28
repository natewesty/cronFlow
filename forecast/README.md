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

Each step logs its wall time (`step=load|select|daily|backtest|monthly|periods|bottles|clubs|write <seconds>s`); the club step also logs `club tiers=… active_in_listed_tiers=… members=…` to compare listed-tier actives with `members_a0`.

Timings (2026-10-27 production dry run, origin 2026-10-26): load 4.9 s, select 5.2 s (two selection backtests + grid + headline guard), daily 0.05 s, backtest 2.4 s (final, on champions), monthly 0.2 s, periods 0.5 s, bottles 0.4 s, clubs 4.3 s; total ≈ 18 s. Champions changed on 6 of 14 selectable KPIs (4, 22, 30 blends; 8 blend 0.25; 28 level; 7 calibrated swm); the headline guard reverted KPI 2's blend to protect Average Tasting Fee (20).

Acceptance (2026-10-27, current dry run with Bottles v2: run time 19 s, `bottle_rows` 15,365): month ahead=1 WMAPE kpi 4 = 0.080, kpi 6 = 0.083 (target ≤ 0.15), kpi 14 = 0.108 (previous engine 0.185; v2 run 7 gave 0.085, 0.088 and 0.114). Daily 8–28 WMAPE (previous → v2): kpi 4 0.316 → 0.148, kpi 6 0.348 → 0.165, kpi 13 0.92 → 0.53, kpi 14 0.413 → 0.332, kpi 5 1.82 → 1.18.

Bottle release rule: a vintage releases in its first month with ≥ 50 bottles. A vintage that never reached 50 releases on its first sale when no vintage of its label ever did (small labels, magnums) or when it is older than the label's newest vintage that did (library tail); otherwise it is unreleased (stray pre-release bottles): no release month, forecast as an incoming vintage whose lifetime already includes the bottles sold, and the default next vintage counts from the newest released one.

## Bottles v2: tables, channels, stock rules

Tables (all in `forecast`; schema.sql creates them on `forecast schema` and every run):

- `bottle_history` (label, vintage, varietal, month_start, channel, bottles, revenue): the engine's own bottle history, deleted and rewritten every non-dry run from the same frame the forecast uses (written by `io.write_bottle_history`, chunked). The app reads it for actuals and for channel shares.
- `stock_snapshot` and `stock_snapshot_row`: one row per uploaded Groskopf inventory pull and its parts (cases and bottles on hand, 9L-case equivalents, matched label). Written by the app upload, never by the engine.
- `wine_code_map` (wine code → label, varietal): app-maintained mapping used when matching snapshot rows to labels.
- `stock_plan` (plan_year, label, vintage; `usable_9l`, `shiners_9l`, `produced_9l`, `count_shiners`, `release`, `release_date`): the per-vintage stock the plan year is built on. The engine only reads it (`io.load_stock_plan`, joined to the label's `label_plan` status). A missing table logs one warning and loads empty.
- `allocation_plan` (plan_year, label, vintage, channel, `cases_9l`, `source`, `model_cases_9l`) and `channel_share` (plan_year, channel, share): app-written allocation of the stock across channels and the channel mix. The engine does not read them.

Channels: five, `Club`, `POS`, `Event`, `Web`, `Inbound` (`BOTTLE_CHANNELS`). `Event` is split out of POS: a POS line whose `event_fee_or_wine` is `'Event Wine'` becomes `Event` in `bottle_history`; everything else on POS stays `POS`. Profiles (label, tier and global curves), the vintage forecast and `bottle_forecast` all carry the five channels; a label's curve still sums to 1 over channels and months. Production dry run (2026-10-27): 875 Event rows (11,671 bottles) in the history, and `bottle_rows` rose from 12,292 to 15,365.

Stock rules (`forecast_vintages(stock_plan=…)`, the plan year's rows only; detail in the `bottles.py` docstring):

- stock_bottles = (`usable_9l` + `shiners_9l` when `count_shiners`) × bpc when `usable_9l` is known (a warehouse count), else `produced_9l` × bpc (a typed row); bpc is 6 for a large format, else 12.
- Candidates: every row whose label_plan status is not `retire` and with at least 5 cases (`STOCK_MIN_CASES`) is a candidate, including vintages past the 36-month curve.
- Cap (replaces `cases_made` × 12 for that vintage): `sold_to_date` + stock_bottles for a released vintage or any warehouse-counted row (counted after any sales); stock_bottles alone for an unreleased typed row. Lifetime p50 is min(lifetime, cap), p90 ≤ max(cap, p50).
- Release: an unreleased candidate releases on the stock `release_date` (month start), else the label_plan `release_month`, else the default rule. A stock date counts as a planned date for the "not after origin" clamp.
- Beyond-window tail: a released vintage with a stock row and `months_sold` ≥ 36 sells on at its own trailing-12 pace per channel (from the month after origin) until the stock runs out; the crossing month is truncated, so the tail never exceeds the stock. It needs no profile and is logged once per label.
- No stock row (or an empty `stock_plan`): nothing changes.

## v3 champion models

Each selectable KPI (`SELECTABLE_KPIS`: 1, 2, 4, 7, 8, 13, 15, 16, 18, 22, 28, 29, 30, 31) gets a champion: organic = `m x (w x year-ago model + (1 - w) x recent-level model)`.

- Candidates: the year-ago seasonal-weighted model (`swm`, the default) and a recent-level model scaled by a seasonal index.
- Grid: `w` in `champion_weights` (default 0, 0.25, 0.5, 0.75, 1; 1.0 is always present), each uncalibrated and with a leave-one-out bias calibration `m` (shrunk by `calibration_shrink`, skipped when the live ratio is off by more than `calibration_max`).
- Selection: two rolling backtests (year-ago and level), month-ahead=1 points per KPI, wMAPE per grid cell. The default is replaced only by a challenger at least `champion_margin` (0.005) better with at least `champion_min_months` (6) months. Ties go to the cell nearest the default (higher `w`, uncalibrated first). Months where the actual and both forecasts are zero are dropped after the two frames are joined.
- Club KPIs (4, 13, 22, 28) never adopt a calibrated cell: selection scores the whole period value (organic + release) while the live chain applies `m` to the organic residual only. Their calibrated cells are still scored and kept in `candidates`.
- Headline guard: after per-KPI selection, the headline KPIs in `HEADLINE_GUARD_KPIS` (3, 6, 14, 20, 31) are re-scored on month-ahead=1 period values rebuilt from their parts (`SUM_IDENTITIES`, or Σ numerators ÷ Σ denominators × scale for `RATIO_IDENTITIES`; 31 has no identity and is its own part; parts that are not selectable keep their year-ago values) under the chosen champions and under the defaults. While one is worse than the default by more than `champion_margin`, the selectable part whose reversion improves it most goes back to the default (its grid and `wmape_default` kept) and everything is re-scored. The run log names each reversion (`champion kpi=2 reverted for kpi=20`); `forecast_models` is unchanged.
- Final backtest: rerun with the champions in place; its month-ahead=1 wMAPE fills `forecast_models.wmape` (`wmape_default` is the year-ago model on the selection backtest). The daily bands and the period tables come from this backtest.
- Calendar flag: backtest origins that have a stored booking snapshot use it as on-the-books and are flagged `calendar` in `forecast_backtest_periods`.
- `forecast_models` holds one row per selectable KPI per run (`model`, `w_swm`, `calibration`, `wmape`, `wmape_default`, `wmape_selection`, `n`, `candidates`); the champions are also recorded in `forecast_runs.params` under `champions`.
- The monthly path mirrors the champion for future months (the current month keeps the daily path): level = mean of the last three complete months × the year-ago change from those months to the target month (`mh[month − 12k] ÷ mean of the base months' values 12k months back`, with the smallest k ∈ {1, 2} for which all four months exist, so months 13–24 past the base use the two-year lag; clipped 0.4–2.5, shrunk by `lambda_shrink`; 1.0 when neither lag works).
- `--skip-backtest` selects nothing: every KPI stays on the default model and no `forecast_models` rows are written. `--dry-run` logs one `champion kpi=... model=... w=... cal=... wmape=... default=...` line per changed KPI and `champions: X of Y KPIs changed`.
- Step order adds `select` after `load`.

KPI 28 "Total Orders" and KPI 29 "Average Order Value" come from `daily_order_metrics` in `models/kpi/fact_kpi_daily.sql`, which excludes tasting-lounge orders, event orders and external vendors. Since October 2025 the daily count is lower than Tasting Room Orders (KPI 30) on most days, which is impossible for a true total: 28 is effectively web, phone and club orders. The engine treats it as a small channel; the definition is left to the user (rename, or widen the filter in dbt).

Timings (2026-10-27 production dry run, origin 2026-10-26): load 5.2 s, select 5.2 s (two selection backtests + grid), daily 0.05 s, backtest 2.4 s (final, on champions), monthly 0.2 s, periods 0.5 s, bottles 0.4 s, clubs 4.8 s; total ≈ 19 s. Champions changed on 8 of 14 selectable KPIs (2, 4, 22, 30 blends; 8 blend 0.25; 28 level; 7 and 13 calibrated swm). These figures predate the club-calibration refusal, the headline guard and the monthly level-index fix (2026-10-27 final review): KPI 13 can no longer take a calibrated cell, and the guard may revert KPI 2.

Acceptance (2026-10-27, final-backtest month-ahead wMAPE, v2 run 7 → v3): KPI 14 0.114 → 0.108, KPI 4 0.085 → 0.080, KPI 6 0.088 → 0.083, KPI 7 0.361 → 0.326, KPI 8 0.497 → 0.316, KPI 22 0.083 → 0.073, KPI 28 0.192 → 0.135, KPI 30 0.086 → 0.072; KPI 3 and 20 unchanged (0.118, 0.131); no selectable KPI worse than run 7; event KPIs unchanged. Hidden ratio KPI 21 moves 0.112 → 0.122 with KPI 30's champion (not guarded).

## Known limitations

- Period p10/p90 bands are in-sample: the ratio percentiles come from the same backtest periods they are applied to.
- An assumed schedule date more than 7 days from an actual release double-counts fees (user-input error).
- One-off products (verticals, trios, etches) and large formats without a base-label profile get no bottle forecast and log "no profile … skipped".
- An under-threshold newest vintage stays unreleased until it sells ≥ 50 in a month or in ≥ 3 months.
- Detail tables of non-latest runs are pruned after 14 days.
- Selection scores linear blends of the two backtest frames (`w·swm + (1 − w)·level`, × `m`), but the live chain floors after blending: on-the-books floors (`max(blend, otb)`) and `apply_floor` are non-linear. On calendar-backed origins (and wherever a floor binds) the grid score can drift from what the live chain forecasts; the final backtest runs the real chain, so the reported `wmape` stays honest.

## Tests

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

## Deploy

The `cronDon-forecast` cron runs on Render daily at 14:00 UTC (created by hand, mirrored in `render.yaml` for reference; there is no Blueprint). It carries the same database env vars as `cronDon` plus `KPI_SCHEMA`.
