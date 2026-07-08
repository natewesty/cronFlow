{{
  config(
    materialized='table'
  )
}}

-- Daily point-in-time count of active club memberships, derived from signup and
-- cancel dates rather than current status so historical days remain correct after
-- later cancellations. Feeds the dashboard's Active Club Members card, which also
-- reads the same date one year prior for its YoY comparison.

with date_range as (
    select
        (select current_date_pacific from {{ ref('dim_date') }} limit 1) as current_date,
        date('{{ var('kpi_history_start', '2023-07-01') }}') as init_date
)

select
    dd.date_day,
    count(dcm.membership_id) as total_active_club_membership
from {{ ref('dim_date') }} dd
cross join date_range dr
left join {{ ref('dim_club_membership') }} dcm
    on date(dcm.signup_at) <= dd.date_day
    and (dcm.cancel_at is null or date(dcm.cancel_at) > dd.date_day)
    -- a cancelled membership missing its cancel date must not count as active forever
    and not (dcm.status = 'Cancelled' and dcm.cancel_at is null)
where dd.date_day >= dr.init_date
and dd.date_day <= dr.current_date
group by dd.date_day
