-- models/staging/stg_customer.sql
-- Staging model for customer data with both current and historical club memberships

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        data->>'id' as customer_id,
        data->>'firstName' as first_name,
        data->>'lastName' as last_name,
        data->>'birthDate' as birth_date,
        data->>'city' as city,
        data->>'stateCode' as state,
        data->>'zipCode' as zip_code,
        data->>'countryCode' as country,
        data->>'emailMarketingStatus' as email_marketing_status,
        data->>'lastActivityDate' as last_activity_date,
        data->'orderInformation'->>'lastOrderDate' as last_order_date,
        data->'orderInformation'->>'orderCount' as order_count,
        data->'orderInformation'->>'lifetimeValue' as lifetime_value,
        data->'orderInformation'->>'currentClubTitle' as current_club_title,
        data->'orderInformation'->>'daysInCurrentClub' as days_in_current_club,
        data->'orderInformation'->>'daysInClub' as days_in_club,
        data->'orderInformation'->>'isActiveClubMember' as is_active_club_member,
        data->'emails'->0->>'email' as email,
        data->'phones'->0->>'phone' as phone,
        data->'loginActivity'->>'lastLoginAt' as last_login_at,
        data->>'createdAt' as created_at,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_customer') }}
    {% if is_incremental() %}
        where date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
),

-- Customer-level data (current status)
customer_current as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        is_active_club_member,
        email_marketing_status,
        city,
        state,
        zip_code,
        country,
        last_activity_date,
        last_order_date,
        order_count,
        lifetime_value,
        days_in_current_club,
        days_in_club,
        last_login_at,
        created_at,
        updated_at,
        last_processed_at,
        null as club_id,
        current_club_title as club_title,
        null as signup_date,
        null as cancel_date,
        null as club_membership_id,
        null as is_club_active,
        'current' as data_type
    from base
),

-- Extract club memberships from the clubs array (historical data)
customer_clubs as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.is_active_club_member::text,
        c.email_marketing_status,
        c.city,
        c.state,
        c.zip_code,
        c.country,
        c.last_activity_date,
        c.last_order_date,
        c.order_count,
        c.lifetime_value,
        c.days_in_current_club,
        c.days_in_club,
        c.last_login_at,
        c.created_at,
        c.updated_at,
        c.last_processed_at,
        club->>'clubId' as club_id,
        club->>'clubTitle' as club_title,
        club->>'signupDate' as signup_date,
        club->>'cancelDate' as cancel_date,
        club->>'clubMembershipId' as club_membership_id,
        -- Determine if this specific club membership is active
        case 
            when club->>'cancelDate' is null then '1'
            else '0'
        end as is_club_active,
        'historical' as data_type
    from base c
    cross join jsonb_array_elements(c.data->'clubs') as club
    where c.data->'clubs' is not null
)

-- Union both current and historical data
select * from customer_current
union all
select * from customer_clubs 