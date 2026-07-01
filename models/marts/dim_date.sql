-- Calendar spine: one row per day 2015‑01‑01 → 2035‑12‑31
-- All dates are calculated in Pacific Time to align with US West Coast business hours
-- Fiscal fields derive from the configurable start month (macros/fiscal.sql).

{#-- Resolve the fiscal start month once and thread it through the macros --#}
{% set s = fiscal_start_month() %}

with dates as (
    select
        -- Simple date spine - no timezone conversion needed for calendar dates
        d::date as date_day
    from generate_series(           -- use ASCII "-" in the literals
             date '2015-01-01',
             date '2035-12-31',
             interval '1 day'
         ) d
),
annotated as (
    select
        date_day,
        extract(year    from date_day)::int as year,
        extract(quarter from date_day)::int as quarter,
        extract(month   from date_day)::int as month,
        to_char(date_day, 'Month')          as month_name,
        extract(day     from date_day)::int as day_of_month,
        extract(dow     from date_day)::int as day_of_week,
        to_char(date_day, 'Day')            as weekday_name,
        extract(week    from date_day)::int as iso_week,
        extract(isoyear from date_day)::int as iso_year,
        -- Fiscal year calculations (configurable start month) - using END year naming
        -- e.g., with a July start, July 1, 2025 - June 30, 2026 is "FY2026"
        {{ fiscal_year('date_day', s) }}              as fiscal_year,
        {{ fiscal_year('date_day', s) }} + 1          as fiscal_year_end,
        {{ fiscal_year_name('date_day', s) }}         as fiscal_year_name,
        {{ fiscal_month('date_day', s) }}             as fiscal_month,
        {{ fiscal_quarter('date_day', s) }}           as fiscal_quarter,
        -- Fiscal year boundary date fields (previously only on kpi_dim_date) so
        -- marts can reference a central FY start/end instead of recomputing it.
        {{ fiscal_year_start('date_day', s) }}        as fiscal_year_start,
        {{ fiscal_year_end_boundary('date_day', s) }} as fiscal_year_end_boundary,
        -- Add Pacific Time specific fields for clarity
        'America/Los_Angeles' as timezone,
        -- Current date in Pacific Time for comparison operations
        -- Ensure we get the correct Pacific Time date for comparisons
        (now() AT TIME ZONE 'America/Los_Angeles')::date as current_date_pacific,
        -- Debug: Show the raw timestamp conversion for verification
        now() AT TIME ZONE 'America/Los_Angeles' as current_timestamp_pacific
    from dates
),
fiscal as (
    -- Second stage: derive the fiscal quarter start from the fiscal year start
    -- and fiscal quarter computed above.
    select
        a.*,
        (a.fiscal_year_start + ((a.fiscal_quarter - 1) * interval '3 months'))::date as fiscal_quarter_start
    from annotated a
)
select * from fiscal
