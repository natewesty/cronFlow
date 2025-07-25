-- models/staging/stg_order_event_c7.sql
-- Staging model for Commerce7 event orders

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        data->>'id' as order_id,
        data->>'orderNumber' as order_number,
        data->'metaData'->>'event-fee-or-wine' as event_fee_or_wine,
        data->'metaData'->>'event-specific-sale' as event_specific_sale,
        data->'metaData'->>'event-revenue-relization-date' as event_revenue_realization_date,
        round((data->>'subTotal')::numeric / 100, 2) as subtotal,
        data->>'updatedAt' as updated_at,
        last_processed_at
    from {{ source('raw', 'raw_order') }}
    where jsonb_typeof(data->'metaData') = 'object'
    {% if is_incremental() %}
        and date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
)

select * from base
where event_fee_or_wine is not null and event_fee_or_wine != ''
  and event_specific_sale is not null and event_specific_sale != '' 