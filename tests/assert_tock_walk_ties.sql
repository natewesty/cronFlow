-- The Tock walk's last line must equal what the dashboard shows, for every day.
-- Returns the days on which rec_tock_revenue_daily's dashboard_value differs from agg_daily_revenue's
-- Tock revenue for the same bucket. Any row is a failure: the walk no longer describes the calculation.
-- dbt tests do not run in production; metrics/walk.py makes the same comparison after every run.
with walk as (
    select
        date_day,
        coalesce(sum(dashboard_value) filter (where bucket = 'Tasting Room'), 0) as tasting_room,
        coalesce(sum(dashboard_value) filter (where bucket = 'Club'), 0)         as club,
        coalesce(sum(dashboard_value) filter (where bucket = 'Event'), 0)        as event
    from {{ ref('rec_tock_revenue_daily') }}
    group by date_day
)
select
    a.date_day,
    a.tasting_room_fees_revenue       - coalesce(w.tasting_room, 0) as tasting_room_diff,
    a.wine_club_fees_revenue          - coalesce(w.club, 0)         as club_diff,
    a.event_fees_reservations_revenue - coalesce(w.event, 0)        as event_diff
from {{ ref('agg_daily_revenue') }} a
left join walk w on w.date_day = a.date_day
where abs(a.tasting_room_fees_revenue       - coalesce(w.tasting_room, 0)) > 0.005
   or abs(a.wine_club_fees_revenue          - coalesce(w.club, 0))         > 0.005
   or abs(a.event_fees_reservations_revenue - coalesce(w.event, 0))        > 0.005
