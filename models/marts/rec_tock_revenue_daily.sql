{{
  config(
    materialized='table',
    tags=['calculation_record']
  )
}}

-- Tagged calculation_record: ingest.py keeps this model out of the marts stage and builds it on its
-- own after the kpi stage. It evaluates casts no other model does, and a model that errors in the
-- marts stage stops the kpi stage (the tiles) from running at all.

-- The walk from Tock's own figures to the dashboard's Tock revenue, one row per visit date and
-- attribution bucket. The Calculation Record page sums these rows over a period to show, line by line,
-- how "what Tock charged" becomes "what the dashboard shows":
--
--     ticket_revenue + sales_tax + service_charge + gratuity + order_fees = total_charged
--     total_charged + payment_differences                                 = net_paid
--     net_paid - processing_fees                                          = dashboard_value
--
-- dashboard_value is fct_tock_reservation.final_total itself, and processing_fees is whatever separates
-- it from net_paid, so the last line always equals what agg_daily_revenue sums for the same day and
-- bucket. tests/assert_tock_walk_ties.sql and metrics/walk.py check that on every run.
--
-- cancelled_kept is money kept on cancelled reservations. It is not part of the walk (cancelled
-- reservations are not in fct_tock_reservation); the page states it beside the walk.

with staged as (
    select
        s.tock_reservation_id,
        s.party_state,
        s.experience_name,
        s.reservation_datetime,
        coalesce(s.total_price_cents, 0) / 100.0      as total_charged,
        coalesce(s.tax_cents, 0) / 100.0              as sales_tax,
        coalesce(s.service_charge_cents, 0) / 100.0   as service_charge,
        coalesce(s.gratuity_cents, 0) / 100.0         as gratuity,
        coalesce(s.net_amount_paid_cents, 0) / 100.0  as net_paid,
        case
            when jsonb_typeof(s.fees) = 'array'
            then coalesce((select sum((fee->>'amountCents')::numeric) from jsonb_array_elements(s.fees) fee), 0)
            else 0
        end / 100.0                                   as order_fees
    from {{ ref('stg_tock_reservation') }} s
    where s.reservation_datetime is not null
),

counted as (
    -- Exactly the reservations the dashboard counts, with the dashboard's own value for each.
    select
        date(f.reservation_datetime)                  as date_day,
        coalesce(f.attribution, 'Unassigned')         as bucket,
        1                                             as reservations,
        s.total_charged - s.sales_tax - s.service_charge - s.gratuity - s.order_fees as ticket_revenue,
        s.sales_tax,
        s.service_charge,
        s.gratuity,
        s.order_fees,
        s.total_charged,
        s.net_paid - s.total_charged                  as payment_differences,
        s.net_paid,
        s.net_paid - coalesce(f.final_total, 0)       as processing_fees,
        coalesce(f.final_total, 0)                    as dashboard_value,
        0::numeric                                    as cancelled_kept
    from {{ ref('fct_tock_reservation') }} f
    join staged s on s.tock_reservation_id = f.tock_reservation_id
),

cancelled as (
    select
        date(s.reservation_datetime)                  as date_day,
        coalesce(de.attribution, 'Unassigned')        as bucket,
        0 as reservations,
        0::numeric as ticket_revenue, 0::numeric as sales_tax, 0::numeric as service_charge,
        0::numeric as gratuity, 0::numeric as order_fees, 0::numeric as total_charged,
        0::numeric as payment_differences, 0::numeric as net_paid, 0::numeric as processing_fees,
        0::numeric as dashboard_value,
        s.net_paid                                    as cancelled_kept
    from staged s
    left join {{ ref('dim_experience') }} de on s.experience_name = de.experience
    where s.party_state = 'CANCELLED'
    and s.net_paid <> 0
)

select
    date_day,
    bucket,
    sum(reservations)::int        as reservations,
    sum(ticket_revenue)           as ticket_revenue,
    sum(sales_tax)                as sales_tax,
    sum(service_charge)           as service_charge,
    sum(gratuity)                 as gratuity,
    sum(order_fees)               as order_fees,
    sum(total_charged)            as total_charged,
    sum(payment_differences)      as payment_differences,
    sum(net_paid)                 as net_paid,
    sum(processing_fees)          as processing_fees,
    sum(dashboard_value)          as dashboard_value,
    sum(cancelled_kept)           as cancelled_kept
from (
    select * from counted
    union all
    select * from cancelled
) walk
group by date_day, bucket
