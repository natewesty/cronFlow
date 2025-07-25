-- models/staging/stg_club_membership.sql
-- Staging model for Commerce7 club membership data

{{ config(
    materialized='incremental',
    unique_key='club_membership_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        data->>'id' as club_membership_id,
        data->>'customerId' as customer_id,
        data->>'clubId' as club_id,
        data->'club'->>'title' as club_title,
        data->>'status' as status,
        data->>'orderDeliveryMethod' as order_delivery_method,
        data->>'signupDate' as signup_date,
        data->>'cancelDate' as cancel_date,
        data->>'cancellationReason' as cancellation_reason,
        data->>'cancellationComments' as cancellation_comments,
        data->>'lastProcessedDate' as last_processed_date,
        data->>'currentNumberOfShipments' as current_number_of_shipments,
        data->>'createdAt' as created_at,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_club_membership') }}
    {% if is_incremental() %}
        where date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
)

select * from base 