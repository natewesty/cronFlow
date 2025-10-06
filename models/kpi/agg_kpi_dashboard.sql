{{ config(
    materialized='incremental',
    schema='nate_sandbox',
    unique_key=['as_of_date','kpi_id','entity_id'],
    on_schema_change='ignore',
    incremental_strategy='merge'   -- requires a warehouse that supports MERGE (Postgres via dbt-postgres uses insert+delete emulation)
) }}

{#-----------------------------
 Vars controlling recompute window
------------------------------#}
{% set lookback_days = var('kpi_dashboard_lookback_days', 400) %}  {# for YoY windows #}
{% set backfill_days = var('kpi_dashboard_backfill_days', 1) %}    {# how many as_of_dates to (re)calculate each run #}

with params as (
  select
      {{ dbt.current_timestamp_in_utc() }} as ts_utc
    , current_date::date                   as today
),
as_of_dates as (
  -- Recompute a small range of as_of_dates each run (today minus N days)
  select (today - offs)::date as as_of_date
  from params p
  cross join lateral generate_series(0, {{ backfill_days }}::int, 1) as g(offs)
),
date_filter as (
  -- For performance: limit scanning fact_kpi_daily to just the needed date range
  select
      (select min(as_of_date) from as_of_dates)                                         as min_as_of
    , (select max(as_of_date) from as_of_dates)                                         as max_as_of
),
scan_window as (
  select
      (min_as_of - interval '{{ lookback_days }} days')::date as scan_start
    , max_as_of                                                as scan_end
  from date_filter
),
f as (
  -- Restrict facts to minimal window needed for all calculations
  select fd.*
  from {{ source('nate_sandbox', 'fact_kpi_daily') }} fd
  join scan_window w on fd.date_key between w.scan_start and w.scan_end
),
p as (
  select a.as_of_date from as_of_dates a
),
{{ kpi_bounds_cte('p') }},

current_rollups as (
  select
      f.kpi_id
    , f.entity_id
    , p.as_of_date
    , sum(case when f.date_key between wb.month_start   and p.as_of_date then f.value end) as mtd_value
    , sum(case when f.date_key between wb.quarter_start and p.as_of_date then f.value end) as qtd_value
    , sum(case when f.date_key between wb.year_start    and p.as_of_date then f.value end) as ytd_value
    , sum(case when f.date_key between wb.last28_start  and p.as_of_date then f.value end) as last28_value
  from f
  cross join p
  join window_bounds wb on true
  group by 1,2,3
),
prior_rollups as (
  select
      f.kpi_id
    , f.entity_id
    , p.as_of_date
    , sum(case when f.date_key between wb.prev_month_start   and (wb.prev_month_start   + (p.as_of_date - wb.month_start))   then f.value end) as mtd_prior
    , sum(case when f.date_key between wb.prev_quarter_start and (wb.prev_quarter_start + (p.as_of_date - wb.quarter_start)) then f.value end) as qtd_prior
    , sum(case when f.date_key between wb.prev_year_start    and (wb.prev_year_start    + (p.as_of_date - wb.year_start))    then f.value end) as ytd_prior
    , sum(case when f.date_key between wb.prev_last28_start  and (wb.prev_last28_start + interval '27 days')                then f.value end) as last28_prior
  from f
  cross join p
  join window_bounds wb on true
  group by 1,2,3
),
calc as (
  select
      c.as_of_date
    , c.kpi_id
    , c.entity_id

    , c.mtd_value
    , p.mtd_prior
    , (c.mtd_value - p.mtd_prior)                                              as mtd_delta
    , case when p.mtd_prior = 0 then null else (c.mtd_value - p.mtd_prior)/p.mtd_prior end as mtd_delta_pct

    , c.qtd_value
    , p.qtd_prior
    , (c.qtd_value - p.qtd_prior)                                              as qtd_delta
    , case when p.qtd_prior = 0 then null else (c.qtd_value - p.qtd_prior)/p.qtd_prior end as qtd_delta_pct

    , c.ytd_value
    , p.ytd_prior
    , (c.ytd_value - p.ytd_prior)                                              as ytd_delta
    , case when p.ytd_prior = 0 then null else (c.ytd_value - p.ytd_prior)/p.ytd_prior end as ytd_delta_pct

    , c.last28_value
    , p.last28_prior
    , (c.last28_value - p.last28_prior)                                        as last28_delta
    , case when p.last28_prior = 0 then null else (c.last28_value - p.last28_prior)/p.last28_prior end as last28_delta_pct
  from current_rollups c
  join prior_rollups  p using (as_of_date, kpi_id, entity_id)
),
final as (
  select
      as_of_date
    , kpi_id
    , entity_id

    , mtd_value,  mtd_prior,  mtd_delta,  mtd_delta_pct
    , qtd_value,  qtd_prior,  qtd_delta,  qtd_delta_pct
    , ytd_value,  ytd_prior,  ytd_delta,  ytd_delta_pct
    , last28_value, last28_prior, last28_delta, last28_delta_pct

    , jsonb_build_object(
        'as_of', as_of_date,
        'mtd',    jsonb_build_object('v', mtd_value,    'p', mtd_prior,    'd', mtd_delta,    'dp', mtd_delta_pct),
        'qtd',    jsonb_build_object('v', qtd_value,    'p', qtd_prior,    'd', qtd_delta,    'dp', qtd_delta_pct),
        'ytd',    jsonb_build_object('v', ytd_value,    'p', ytd_prior,    'd', ytd_delta,    'dp', ytd_delta_pct),
        'last28', jsonb_build_object('v', last28_value, 'p', last28_prior, 'd', last28_delta, 'dp', last28_delta_pct)
      )::jsonb as payload
  from calc
)

select * from final

{% if is_incremental() %}
  -- Limit the merge scope to just the backfill window
  where as_of_date in (select as_of_date from as_of_dates)
{% endif %}
