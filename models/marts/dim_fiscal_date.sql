-- models/marts/dim_fiscal_date.sql
-- Fiscal date dimension table for mapping calendar dates to fiscal periods

{{ config(
    materialized='incremental',
    unique_key='calendar_date',
    incremental_strategy='merge'
) }}

with recursive date_spine as (
    select date('2020-01-01')::date as date_value
    union all
    select (date_value + interval '1 day')::date
    from date_spine
    where date_value < date('2030-12-31')  -- 10 years of dates
),

fiscal_dates as (
    select
        date_value as calendar_date,
        case 
            when extract(month from date_value) >= 7 
            then extract(year from date_value) + 1
            else extract(year from date_value)
        end as fiscal_year,
        case 
            when extract(month from date_value) >= 7 
            then extract(month from date_value) - 6
            else extract(month from date_value) + 6
        end as fiscal_month,
        case 
            when extract(month from date_value) >= 7 
            then 'FY' || (extract(year from date_value) + 1)
            else 'FY' || extract(year from date_value)
        end as fiscal_year_label,
        case 
            when extract(month from date_value) >= 7 
            then date(extract(year from date_value) || '-07-01')
            else date((extract(year from date_value) - 1) || '-07-01')
        end as fiscal_year_start_date,
        case 
            when extract(month from date_value) >= 7 
            then date((extract(year from date_value) + 1) || '-06-30')
            else date(extract(year from date_value) || '-06-30')
        end as fiscal_year_end_date
    from date_spine
)

select * from fiscal_dates
{% if is_incremental() %}
    where calendar_date > (select max(calendar_date) from {{ this }})
{% endif %} 