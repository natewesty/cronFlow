-- models/staging/stg_club_membership_events.sql
-- Pre-processed club membership events for efficient fact table generation
-- FILTERED: Only processes specific clubs: The Estate Club, The Estate Club Plus, Premier Cru 4 *, Premier Cru 6 *, Grand Cru 4 *, Grand Cru 6*
-- This staging model reduces the computational load by pre-calculating:
-- 1. Date ranges for each club
-- 2. Daily signup and cancellation events
-- 3. Customer membership status changes

{{ config(
    materialized='incremental',
    unique_key=['data_type', 'club_title', 'event_date', 'customer_id'],
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

base_memberships as (
    select
        customer_id,
        club_title,
        -- Normalize date format and extract date portion
        case 
            when signup_date is not null 
            then substr(signup_date, 1, 10)
            else null 
        end as signup_date,
        case 
            when cancel_date is not null 
            then substr(cancel_date, 1, 10)
            else null 
        end as cancel_date,
        -- Effective cancel date for active memberships
        coalesce(substr(cancel_date, 1, 10), '2099-12-31') as effective_cancel_date,
        updated_at,
        last_processed_at
    from {{ ref('stg_club_membership') }}
    where signup_date is not null
        and club_title is not null
        and club_title in (select club_title from target_clubs)
    {% if is_incremental() %}
        and date(last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
),

-- Calculate club date ranges (minimal date spine)
club_date_ranges as (
    select
        club_title,
        min(signup_date) as club_start_date,
        max(effective_cancel_date) as club_end_date,
        max(updated_at) as updated_at,
        max(last_processed_at) as last_processed_at
    from base_memberships
    group by club_title
),

-- Calculate active member counts separately
active_member_counts as (
    select
        club_title,
        count(distinct customer_id)::text as total_active_members,
        max(updated_at) as updated_at,
        max(last_processed_at) as last_processed_at
    from base_memberships
    where cancel_date is null
    group by club_title
),

-- Pre-calculate daily signup events
daily_signup_events as (
    select
        club_title,
        signup_date as event_date,
        count(*)::text as new_signups,
        count(distinct customer_id)::text as unique_signups,
        max(updated_at) as updated_at,
        max(last_processed_at) as last_processed_at
    from base_memberships
    where signup_date is not null
    group by club_title, signup_date
),

-- Pre-calculate daily cancellation events
daily_cancellation_events as (
    select
        club_title,
        cancel_date as event_date,
        count(*)::text as cancellations,
        count(distinct customer_id)::text as unique_cancellations,
        max(updated_at) as updated_at,
        max(last_processed_at) as last_processed_at
    from base_memberships
    where cancel_date is not null
    group by club_title, cancel_date
),

-- Pre-calculate membership status changes (for efficient daily calculation)
membership_status_changes as (
    select
        customer_id,
        club_title,
        signup_date,
        cancel_date,
        effective_cancel_date,
        -- Flag for active membership periods
        case 
            when cancel_date is null then '1'
            else '0'
        end as is_currently_active,
        updated_at,
        last_processed_at
    from base_memberships
)

select 
    'club_ranges' as data_type,
    cdr.club_title,
    cdr.club_start_date,
    cdr.club_end_date,
    amc.total_active_members,
    null as event_date,
    null as customer_id,
    null as new_signups,
    null as cancellations,
    null as unique_signups,
    null as unique_cancellations,
    null as signup_date,
    null as cancel_date,
    null as effective_cancel_date,
    null as is_currently_active,
    cdr.updated_at,
    cdr.last_processed_at
from club_date_ranges cdr
left join active_member_counts amc on cdr.club_title = amc.club_title

union all

select 
    'signups' as data_type,
    club_title,
    null as club_start_date,
    null as club_end_date,
    null as total_active_members,
    event_date,
    null as customer_id,
    new_signups,
    null as cancellations,
    unique_signups,
    null as unique_cancellations,
    null as signup_date,
    null as cancel_date,
    null as effective_cancel_date,
    null as is_currently_active,
    updated_at,
    last_processed_at
from daily_signup_events

union all

select 
    'cancellations' as data_type,
    club_title,
    null as club_start_date,
    null as club_end_date,
    null as total_active_members,
    event_date,
    null as customer_id,
    null as new_signups,
    cancellations,
    null as unique_signups,
    unique_cancellations,
    null as signup_date,
    null as cancel_date,
    null as effective_cancel_date,
    null as is_currently_active,
    updated_at,
    last_processed_at
from daily_cancellation_events

union all

select 
    'memberships' as data_type,
    club_title,
    null as club_start_date,
    null as club_end_date,
    null as total_active_members,
    null as event_date,
    customer_id,
    null as new_signups,
    null as cancellations,
    null as unique_signups,
    null as unique_cancellations,
    signup_date,
    cancel_date,
    effective_cancel_date,
    is_currently_active,
    updated_at,
    last_processed_at
from membership_status_changes 