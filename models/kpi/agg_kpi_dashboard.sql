{{ config(
    materialized='incremental',
    unique_key=['as_of_date','kpi_id','entity_id'],
    on_schema_change='ignore',
    incremental_strategy='merge',
    schema='nate_sandbox'
) }}

{#-----------------------------
 Vars controlling recompute window
------------------------------#}
{% set lookback_days = var('kpi_dashboard_lookback_days', 730) %}  {# for YoY windows #}
{% set backfill_days = var('kpi_dashboard_backfill_days', 1) %}    {# how many as_of_dates to (re)calculate each run #}

with params as (
  select
      current_timestamp as ts_utc
    , current_date::date as today
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
  from {{ ref('fact_kpi_daily') }} fd
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
    , wb.as_of_date
    , wb.fiscal_year
    -- Period metrics
    , sum(case when f.date_key between wb.month_start        and wb.as_of_date then f.value end) as mtd_value
    , sum(case when f.date_key between wb.quarter_start      and wb.as_of_date then f.value end) as qtd_value
    , sum(case when f.date_key between wb.fiscal_year_start  and wb.as_of_date then f.value end) as ytd_value
    , sum(case when f.date_key between wb.last28_start       and wb.as_of_date then f.value end) as last28_value
    -- Fiscal month totals (Jul-Jun)
    , sum(case when extract(month from f.date_key) = 7  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as jul_value
    , sum(case when extract(month from f.date_key) = 8  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as aug_value
    , sum(case when extract(month from f.date_key) = 9  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as sep_value
    , sum(case when extract(month from f.date_key) = 10 and extract(year from f.date_key) = wb.fiscal_year then f.value end) as oct_value
    , sum(case when extract(month from f.date_key) = 11 and extract(year from f.date_key) = wb.fiscal_year then f.value end) as nov_value
    , sum(case when extract(month from f.date_key) = 12 and extract(year from f.date_key) = wb.fiscal_year then f.value end) as dec_value
    , sum(case when extract(month from f.date_key) = 1  and extract(year from f.date_key) = wb.fiscal_year + 1 then f.value end) as jan_value
    , sum(case when extract(month from f.date_key) = 2  and extract(year from f.date_key) = wb.fiscal_year + 1 then f.value end) as feb_value
    , sum(case when extract(month from f.date_key) = 3  and extract(year from f.date_key) = wb.fiscal_year + 1 then f.value end) as mar_value
    , sum(case when extract(month from f.date_key) = 4  and extract(year from f.date_key) = wb.fiscal_year + 1 then f.value end) as apr_value
    , sum(case when extract(month from f.date_key) = 5  and extract(year from f.date_key) = wb.fiscal_year + 1 then f.value end) as may_value
    , sum(case when extract(month from f.date_key) = 6  and extract(year from f.date_key) = wb.fiscal_year + 1 then f.value end) as jun_value
  from f
  cross join window_bounds wb
  group by 1,2,3,4
),
prior_rollups as (
  select
      f.kpi_id
    , f.entity_id
    , wb.as_of_date
    , wb.fiscal_year
    -- Period metrics
    , sum(case when f.date_key between wb.prev_month_start        and (wb.prev_month_start        + (wb.as_of_date - wb.month_start))        then f.value end) as mtd_prior
    , sum(case when f.date_key between wb.prev_quarter_start      and (wb.prev_quarter_start      + (wb.as_of_date - wb.quarter_start))      then f.value end) as qtd_prior
    , sum(case when f.date_key between wb.prev_fiscal_year_start  and (wb.prev_fiscal_year_start  + (wb.as_of_date - wb.fiscal_year_start))  then f.value end) as ytd_prior
    , sum(case when f.date_key between wb.prev_last28_start       and (wb.prev_last28_start       + interval '27 days')                      then f.value end) as last28_prior
    -- Prior fiscal month totals (Jul-Jun from prior year)
    , sum(case when extract(month from f.date_key) = 7  and extract(year from f.date_key) = wb.fiscal_year - 1 then f.value end) as jul_prior
    , sum(case when extract(month from f.date_key) = 8  and extract(year from f.date_key) = wb.fiscal_year - 1 then f.value end) as aug_prior
    , sum(case when extract(month from f.date_key) = 9  and extract(year from f.date_key) = wb.fiscal_year - 1 then f.value end) as sep_prior
    , sum(case when extract(month from f.date_key) = 10 and extract(year from f.date_key) = wb.fiscal_year - 1 then f.value end) as oct_prior
    , sum(case when extract(month from f.date_key) = 11 and extract(year from f.date_key) = wb.fiscal_year - 1 then f.value end) as nov_prior
    , sum(case when extract(month from f.date_key) = 12 and extract(year from f.date_key) = wb.fiscal_year - 1 then f.value end) as dec_prior
    , sum(case when extract(month from f.date_key) = 1  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as jan_prior
    , sum(case when extract(month from f.date_key) = 2  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as feb_prior
    , sum(case when extract(month from f.date_key) = 3  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as mar_prior
    , sum(case when extract(month from f.date_key) = 4  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as apr_prior
    , sum(case when extract(month from f.date_key) = 5  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as may_prior
    , sum(case when extract(month from f.date_key) = 6  and extract(year from f.date_key) = wb.fiscal_year then f.value end) as jun_prior
  from f
  cross join window_bounds wb
  group by 1,2,3,4
),
calc as (
  select
      c.as_of_date
    , c.kpi_id
    , c.entity_id

    -- Period metrics
    , c.mtd_value,  p.mtd_prior,  (c.mtd_value - p.mtd_prior) as mtd_delta,  case when p.mtd_prior = 0 then null else (c.mtd_value - p.mtd_prior)/p.mtd_prior end as mtd_delta_pct
    , c.qtd_value,  p.qtd_prior,  (c.qtd_value - p.qtd_prior) as qtd_delta,  case when p.qtd_prior = 0 then null else (c.qtd_value - p.qtd_prior)/p.qtd_prior end as qtd_delta_pct
    , c.ytd_value,  p.ytd_prior,  (c.ytd_value - p.ytd_prior) as ytd_delta,  case when p.ytd_prior = 0 then null else (c.ytd_value - p.ytd_prior)/p.ytd_prior end as ytd_delta_pct
    , c.last28_value, p.last28_prior, (c.last28_value - p.last28_prior) as last28_delta, case when p.last28_prior = 0 then null else (c.last28_value - p.last28_prior)/p.last28_prior end as last28_delta_pct

    -- Monthly metrics (Jul-Jun)
    , c.jul_value, p.jul_prior, (c.jul_value - p.jul_prior) as jul_delta, case when p.jul_prior = 0 then null else (c.jul_value - p.jul_prior)/p.jul_prior end as jul_delta_pct
    , c.aug_value, p.aug_prior, (c.aug_value - p.aug_prior) as aug_delta, case when p.aug_prior = 0 then null else (c.aug_value - p.aug_prior)/p.aug_prior end as aug_delta_pct
    , c.sep_value, p.sep_prior, (c.sep_value - p.sep_prior) as sep_delta, case when p.sep_prior = 0 then null else (c.sep_value - p.sep_prior)/p.sep_prior end as sep_delta_pct
    , c.oct_value, p.oct_prior, (c.oct_value - p.oct_prior) as oct_delta, case when p.oct_prior = 0 then null else (c.oct_value - p.oct_prior)/p.oct_prior end as oct_delta_pct
    , c.nov_value, p.nov_prior, (c.nov_value - p.nov_prior) as nov_delta, case when p.nov_prior = 0 then null else (c.nov_value - p.nov_prior)/p.nov_prior end as nov_delta_pct
    , c.dec_value, p.dec_prior, (c.dec_value - p.dec_prior) as dec_delta, case when p.dec_prior = 0 then null else (c.dec_value - p.dec_prior)/p.dec_prior end as dec_delta_pct
    , c.jan_value, p.jan_prior, (c.jan_value - p.jan_prior) as jan_delta, case when p.jan_prior = 0 then null else (c.jan_value - p.jan_prior)/p.jan_prior end as jan_delta_pct
    , c.feb_value, p.feb_prior, (c.feb_value - p.feb_prior) as feb_delta, case when p.feb_prior = 0 then null else (c.feb_value - p.feb_prior)/p.feb_prior end as feb_delta_pct
    , c.mar_value, p.mar_prior, (c.mar_value - p.mar_prior) as mar_delta, case when p.mar_prior = 0 then null else (c.mar_value - p.mar_prior)/p.mar_prior end as mar_delta_pct
    , c.apr_value, p.apr_prior, (c.apr_value - p.apr_prior) as apr_delta, case when p.apr_prior = 0 then null else (c.apr_value - p.apr_prior)/p.apr_prior end as apr_delta_pct
    , c.may_value, p.may_prior, (c.may_value - p.may_prior) as may_delta, case when p.may_prior = 0 then null else (c.may_value - p.may_prior)/p.may_prior end as may_delta_pct
    , c.jun_value, p.jun_prior, (c.jun_value - p.jun_prior) as jun_delta, case when p.jun_prior = 0 then null else (c.jun_value - p.jun_prior)/p.jun_prior end as jun_delta_pct
  from current_rollups c
  join prior_rollups  p using (as_of_date, kpi_id, entity_id)
),
final as (
  select
      as_of_date
    , kpi_id
    , entity_id

    -- Period metrics
    , mtd_value,  mtd_prior,  mtd_delta,  mtd_delta_pct
    , qtd_value,  qtd_prior,  qtd_delta,  qtd_delta_pct
    , ytd_value,  ytd_prior,  ytd_delta,  ytd_delta_pct
    , last28_value, last28_prior, last28_delta, last28_delta_pct

    -- Monthly metrics
    , jul_value, jul_prior, jul_delta, jul_delta_pct
    , aug_value, aug_prior, aug_delta, aug_delta_pct
    , sep_value, sep_prior, sep_delta, sep_delta_pct
    , oct_value, oct_prior, oct_delta, oct_delta_pct
    , nov_value, nov_prior, nov_delta, nov_delta_pct
    , dec_value, dec_prior, dec_delta, dec_delta_pct
    , jan_value, jan_prior, jan_delta, jan_delta_pct
    , feb_value, feb_prior, feb_delta, feb_delta_pct
    , mar_value, mar_prior, mar_delta, mar_delta_pct
    , apr_value, apr_prior, apr_delta, apr_delta_pct
    , may_value, may_prior, may_delta, may_delta_pct
    , jun_value, jun_prior, jun_delta, jun_delta_pct

    -- Optional: Lightweight JSON payload for API convenience (periods only, not months)
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

