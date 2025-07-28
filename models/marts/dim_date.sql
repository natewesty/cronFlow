with dates as (
    select date_trunc('day', d)::date as date_day
    from generate_series('2015‑01‑01'::date
                       , '2035‑12‑31'::date
                       , interval '1 day') d
)
select
    date_day,
    extract(year    from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month   from date_day)::int as month,
    to_char(date_day,'Month')           as month_name,
    extract(day     from date_day)::int as day_of_month,
    extract(dow     from date_day)::int as day_of_week,
    to_char(date_day,'Day')             as weekday_name,
    extract(week    from date_day)::int as iso_week,       -- ✅ fixed
    extract(isoyear from date_day)::int as iso_year        -- optional
from dates
