-- Calendar spine: one row per day 2015‑01‑01 → 2035‑12‑31
-- All dates are calculated in Pacific Time to align with US West Coast business hours
with dates as (
    select 
        -- Simple date spine - no timezone conversion needed for calendar dates
        d::date as date_day
    from generate_series(           -- use ASCII "-" in the literals
             date '2015-01-01',
             date '2035-12-31',
             interval '1 day'
         ) d
)

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
    -- Fiscal year calculations (FY starts July 1st) - all in Pacific Time
    case 
        when extract(month from date_day) >= 7 
        then extract(year from date_day) 
        else extract(year from date_day) - 1 
    end::int as fiscal_year,
    case 
        when extract(month from date_day) >= 7 
        then extract(year from date_day) 
        else extract(year from date_day) - 1 
    end::int + 1 as fiscal_year_end,
    'FY' || case 
        when extract(month from date_day) >= 7 
        then extract(year from date_day) 
        else extract(year from date_day) - 1 
    end::text as fiscal_year_name,
    case 
        when extract(month from date_day) >= 7 
        then extract(month from date_day) - 6
        else extract(month from date_day) + 6
    end::int as fiscal_month,
    case 
        when extract(quarter from date_day) >= 3 
        then extract(quarter from date_day) - 2
        else extract(quarter from date_day) + 2
    end::int as fiscal_quarter,
    -- Add Pacific Time specific fields for clarity
    'America/Los_Angeles' as timezone,
    -- Current date in Pacific Time for comparison operations
    -- This is the only place we need timezone conversion since Commerce7 data is already in PST
    (current_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date as current_date_pacific
from dates