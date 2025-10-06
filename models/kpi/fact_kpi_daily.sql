{{
  config(
    materialized='table',
    schema='nate_sandbox'
  )
}}

-- Unpivot agg_daily_revenue into long format for KPI framework
-- This transforms the wide table into (kpi_id, entity_id, date_key, value)

with daily_revenue as (
    select * from {{ ref('agg_daily_revenue') }}
),

unpivoted as (
    -- Tasting Room Metrics
    select date_day as date_key, 1 as kpi_id, 0 as entity_id, tasting_room_wine_revenue as value
    from daily_revenue
    union all
    select date_day, 2, 0, tasting_room_fees_revenue from daily_revenue
    union all
    select date_day, 3, 0, tasting_room_total_revenue from daily_revenue
    
    -- Wine Club Metrics
    union all
    select date_day, 4, 0, wine_club_orders_revenue from daily_revenue
    union all
    select date_day, 5, 0, wine_club_fees_revenue from daily_revenue
    union all
    select date_day, 6, 0, wine_club_total_revenue from daily_revenue
    
    -- Channel Revenue Metrics
    union all
    select date_day, 7, 0, ecomm_revenue from daily_revenue
    union all
    select date_day, 8, 0, phone_revenue from daily_revenue
    
    -- Event Revenue Metrics
    union all
    select date_day, 9, 0, event_fees_orders_revenue from daily_revenue
    union all
    select date_day, 10, 0, event_fees_reservations_revenue from daily_revenue
    union all
    select date_day, 11, 0, event_fees_total_revenue from daily_revenue
    union all
    select date_day, 12, 0, event_wine_revenue from daily_revenue
    
    -- Shipping & Total Revenue
    union all
    select date_day, 13, 0, shipping_revenue from daily_revenue
    union all
    select date_day, 14, 0, total_daily_revenue from daily_revenue
    
    -- Traffic Metrics
    union all
    select date_day, 15, 0, total_reservations from daily_revenue
    union all
    select date_day, 16, 0, total_visitors from daily_revenue
    union all
    select date_day, 17, 0, avg_party_size from daily_revenue
    
    -- Guest Metrics
    union all
    select date_day, 18, 0, tasting_room_guests from daily_revenue
    union all
    select date_day, 19, 0, event_guests from daily_revenue
    union all
    select date_day, 20, 0, avg_tasting_fee_per_guest from daily_revenue
    union all
    select date_day, 21, 0, tasting_room_orders_per_guest_pct from daily_revenue
    
    -- Wine Sales Metrics
    union all
    select date_day, 22, 0, total_9l_sold from daily_revenue
    
    -- Club Membership Metrics
    union all
    select date_day, 23, 0, total_active_club_membership from daily_revenue
    union all
    select date_day, 24, 0, new_member_acquisition from daily_revenue
    union all
    select date_day, 25, 0, existing_member_attrition from daily_revenue
    union all
    select date_day, 26, 0, club_population_net_gain_loss from daily_revenue
    union all
    select date_day, 27, 0, club_conversion_per_taster_pct from daily_revenue
)

select
    date_key,
    kpi_id,
    entity_id,
    coalesce(value, 0) as value
from unpivoted
where date_key is not null
order by date_key, kpi_id

