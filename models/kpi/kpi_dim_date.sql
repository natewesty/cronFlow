{{ config(
    materialized='table',
    on_schema_change='ignore',
    schema='nate_sandbox'
) }}

{#-- Configurable range via vars --#}
{% set start_date = var('dim_date_start', '2015-01-01') %}
{% set end_date   = var('dim_date_end',   '2035-12-31') %}

with base as (
  select d::date as date_key
  from generate_series({{ "'" ~ start_date ~ "'" }}::date, {{ "'" ~ end_date ~ "'" }}::date, interval '1 day') as g(d)
),
annotated as (
  select
      date_key
    , extract(isoyear from date_key)::int            as iso_year
    , extract(year from date_key)::int               as year
    , extract(quarter from date_key)::int            as quarter
    , to_char(date_key,'YYYY-MM')                    as month_ym
    , date_trunc('month',   date_key)::date          as month_start
    , (date_trunc('month',  date_key) + interval '1 month - 1 day')::date   as month_end
    , date_trunc('quarter', date_key)::date          as quarter_start
    , (date_trunc('quarter',date_key) + interval '3 month - 1 day')::date   as quarter_end
    , date_trunc('year',    date_key)::date          as year_start
    , (date_trunc('year',   date_key) + interval '1 year - 1 day')::date    as year_end
    -- Fiscal year starting July 1st
    , case 
        when extract(month from date_key) >= 7 
        then extract(year from date_key)::int
        else extract(year from date_key)::int - 1
      end as fiscal_year
  from base
)
select * from annotated

