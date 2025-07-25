-- models/staging/stg_order.sql
-- Staging model for Commerce7 order data

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        data->>'id' as order_id,
        data->>'orderSubmittedDate' as order_submitted_date,
        data->>'orderPaidDate' as order_paid_date,
        data->>'orderFulfilledDate' as order_fulfilled_date,
        data->>'externalOrderVendor' as external_order_vendor,
        data->>'orderNumber' as order_number,
        data->>'channel' as channel,
        data->>'salesAttributionCode' as sales_attribution_code,
        data->>'orderDeliveryMethod' as order_delivery_method,
        data->>'customerId' as customer_id,
        round((data->>'subTotal')::numeric / 100, 2) as subtotal,
        round((data->>'shipTotal')::numeric / 100, 2) as ship_total,
        data->>'createdAt' as created_at,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_order') }}
    {% if is_incremental() %}
        where date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
)

select * from base 