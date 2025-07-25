-- models/marts/fact_club_membership_daily_optimized.sql
-- Optimized daily fact table for tracking club membership population
-- Uses pre-processed staging data to eliminate cartesian products and improve performance
-- FILTERED: Only includes specific clubs: The Estate Club, The Estate Club Plus, Premier Cru 4 *, Premier Cru 6 *, Grand Cru 4 *, Grand Cru 6 *
-- Key optimizations:
-- 1. Uses stg_club_membership_events for pre-calculated events
-- 2. Eliminates massive cartesian product between dates and customers
-- 3. Pre-calculates date ranges and event aggregations
-- 4. Uses incremental approach for daily calculations

{{ config(
    materialized='incremental',
    unique_key=['calendar_date', 'club_title'],
    incremental_strategy='merge'
) }}

-- Define the specific clubs we want to include (SQLite compatible syntax)
with target_clubs as (
    select 'The Estate Club' as club_title
    union all
    select 'The Estate Club Plus'
    union all
    select 'Premier Cru 4 *'
    union all
    select 'Premier Cru 6 *'
    union all
    select 'Grand Cru 4 *'
    union all
    select 'Grand Cru 6 *'
),

-- Extract pre-calculated data from staging (filtered for target clubs)
club_ranges as (
    select
        club_title,
        club_start_date,
        club_end_date,
        total_active_members,
        last_processed_at
    from {{ ref('stg_club_membership_events') }}
    where data_type = 'club_ranges'
        and club_title in (select club_title from target_clubs)
),

daily_signups as (
    select
        club_title,
        event_date,
        new_signups,
        unique_signups,
        last_processed_at
    from {{ ref('stg_club_membership_events') }}
    where data_type = 'signups'
        and club_title in (select club_title from target_clubs)
),

daily_cancellations as (
    select
        club_title,
        event_date,
        cancellations,
        unique_cancellations,
        last_processed_at
    from {{ ref('stg_club_membership_events') }}
    where data_type = 'cancellations'
        and club_title in (select club_title from target_clubs)
),

membership_data as (
    select
        customer_id,
        club_title,
        signup_date,
        cancel_date,
        effective_cancel_date,
        is_currently_active,
        last_processed_at
    from {{ ref('stg_club_membership_events') }}
    where data_type = 'memberships'
        and club_title in (select club_title from target_clubs)
),

-- Generate optimized date spine (only for actual club activity periods)
optimized_date_spine as (
    select 
        cr.club_title,
        dfd.calendar_date,
        dfd.fiscal_year,
        dfd.fiscal_month,
        dfd.fiscal_year_label,
        to_char(dfd.calendar_date, 'YYYY-MM') as year_month,
        to_char(dfd.calendar_date, 'YYYY-MM-DD') as date_key,
        cr.last_processed_at
    from club_ranges cr
    cross join {{ ref('dim_fiscal_date') }} dfd
    where dfd.calendar_date >= cr.club_start_date::date
        and dfd.calendar_date <= cr.club_end_date::date
        and dfd.calendar_date <= date('2025-06-06')  -- Hard stop for data collection
    {% if is_incremental() %}
        and date_trunc('day', cr.last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
),

-- Calculate daily active memberships using efficient date range logic
-- Instead of cartesian product, use date range checks
daily_active_memberships as (
    select
        ods.calendar_date,
        ods.date_key,
        ods.year_month,
        ods.fiscal_year,
        ods.fiscal_month,
        ods.fiscal_year_label,
        ods.club_title,
        count(md.customer_id) as active_memberships,
        ods.last_processed_at
    from optimized_date_spine ods
    left join membership_data md
        on ods.club_title = md.club_title
        and md.signup_date::date <= ods.calendar_date
        and md.effective_cancel_date::date > ods.calendar_date
    group by 
        ods.calendar_date,
        ods.date_key,
        ods.year_month,
        ods.fiscal_year,
        ods.fiscal_month,
        ods.fiscal_year_label,
        ods.club_title,
        ods.last_processed_at
),

-- Calculate day-over-day changes efficiently
daily_changes as (
    select
        *,
        -- Day-over-day change
        active_memberships - lag(active_memberships) over (
            partition by club_title 
            order by calendar_date
        ) as net_membership_change,
        -- Daily growth rate
        case 
            when lag(active_memberships) over (
                partition by club_title 
                order by calendar_date
            ) > 0 
            then round(
                (active_memberships - lag(active_memberships) over (
                    partition by club_title 
                    order by calendar_date
                )) * 100.0 / lag(active_memberships) over (
                    partition by club_title 
                    order by calendar_date
                ), 2
            )
            else null
        end as daily_growth_rate_pct
    from daily_active_memberships
),

-- Final optimized metrics calculation
final_metrics as (
    select
        dc.calendar_date,
        dc.date_key,
        dc.year_month,
        dc.fiscal_year,
        dc.fiscal_month,
        dc.fiscal_year_label,
        dc.club_title,
        dc.active_memberships,
        dc.net_membership_change,
        dc.daily_growth_rate_pct,
        coalesce(ds.new_signups, '0') as new_signups,
        coalesce(dcanc.cancellations, '0') as cancellations,
        -- 7-day moving average (optimized)
        avg(dc.active_memberships) over (
            partition by dc.club_title 
            order by dc.calendar_date 
            rows between 6 preceding and current row
        ) as active_memberships_7d_ma,
        -- 30-day moving average (optimized)
        avg(dc.active_memberships) over (
            partition by dc.club_title 
            order by dc.calendar_date 
            rows between 29 preceding and current row
        ) as active_memberships_30d_ma,
        -- Growth acceleration
        dc.daily_growth_rate_pct - lag(dc.daily_growth_rate_pct) over (
            partition by dc.club_title 
            order by dc.calendar_date
        ) as growth_acceleration_pct,
        dc.last_processed_at
    from daily_changes dc
    left join daily_signups ds
        on dc.club_title = ds.club_title
        and dc.calendar_date = ds.event_date::date
    left join daily_cancellations dcanc
        on dc.club_title = dcanc.club_title
        and dc.calendar_date = dcanc.event_date::date
    where dc.club_title is not null
        and dc.club_title in (select club_title from target_clubs)
)

select * from final_metrics
order by club_title, calendar_date 