{{ config(
    materialized='table',
    on_schema_change='ignore',
    schema='nate_sandbox'
) }}

{#-- Configurable range via vars --#}
{% set start_date = var('dim_date_start', '2015-01-01') %}
{% set end_date   = var('dim_date_end',   '2035-12-31') %}

-- Unified date dimension replicating all fields from dim_date.sql
-- plus additional boundary fields for KPI calculations
with base as (
  select d::date as date_day
  from generate_series({{ "'" ~ start_date ~ "'" }}::date, {{ "'" ~ end_date ~ "'" }}::date, interval '1 day') as g(d)
),
annotated as (
  select
      -- Primary key
      date_day
      
    -- Standard calendar fields (matching dim_date.sql)
    , extract(year    from date_day)::int               as year
    , extract(quarter from date_day)::int               as quarter
    , extract(month   from date_day)::int               as month
    , to_char(date_day, 'Month')                        as month_name
    , extract(day     from date_day)::int               as day_of_month
    , extract(dow     from date_day)::int               as day_of_week
    , to_char(date_day, 'Day')                          as weekday_name
    , extract(week    from date_day)::int               as iso_week
    , extract(isoyear from date_day)::int               as iso_year
    
    -- Additional calendar boundary fields (KPI-specific)
    , to_char(date_day,'YYYY-MM')                       as month_ym
    , date_trunc('month',   date_day)::date             as month_start
    , (date_trunc('month',  date_day) + interval '1 month - 1 day')::date    as month_end
    , date_trunc('quarter', date_day)::date             as quarter_start
    , (date_trunc('quarter',date_day) + interval '3 month - 1 day')::date    as quarter_end
    , date_trunc('year',    date_day)::date             as year_start
    , (date_trunc('year',   date_day) + interval '1 year - 1 day')::date     as year_end
    
    -- Fiscal year calculations (FY starts July 1st) - matching dim_date.sql
    , case 
        when extract(month from date_day) >= 7 
        then extract(year from date_day) 
        else extract(year from date_day) - 1 
      end::int as fiscal_year
    , case 
        when extract(month from date_day) >= 7 
        then extract(year from date_day) 
        else extract(year from date_day) - 1 
      end::int + 1 as fiscal_year_end
    , 'FY' || case 
        when extract(month from date_day) >= 7 
        then extract(year from date_day) 
        else extract(year from date_day) - 1 
      end::text as fiscal_year_name
    , case 
        when extract(month from date_day) >= 7 
        then extract(month from date_day) - 6
        else extract(month from date_day) + 6
      end::int as fiscal_month
    , case 
        when extract(quarter from date_day) >= 3 
        then extract(quarter from date_day) - 2
        else extract(quarter from date_day) + 2
      end::int as fiscal_quarter
      
    -- Fiscal year boundaries (July 1 - June 30) - KPI-specific
    , case 
        when extract(month from date_day) >= 7 
        then make_date(extract(year from date_day)::int, 7, 1)
        else make_date(extract(year from date_day)::int - 1, 7, 1)
      end as fiscal_year_start_date
    , case 
        when extract(month from date_day) >= 7 
        then make_date(extract(year from date_day)::int + 1, 6, 30)
        else make_date(extract(year from date_day)::int, 6, 30)
      end as fiscal_year_end_date
      
    -- Pacific Time specific fields (matching dim_date.sql)
    , 'America/Los_Angeles' as timezone
    , (now() AT TIME ZONE 'America/Los_Angeles')::date as current_date_pacific
    , now() AT TIME ZONE 'America/Los_Angeles'         as current_timestamp_pacific
      
  from base
)
select * from annotated

