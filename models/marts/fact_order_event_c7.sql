-- models/marts/fact_order_event_c7.sql
-- Order event fact mart from C7

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    order_id,
    event_fee_or_wine,
    event_specific_sale,
    event_revenue_realization_date,
    ROUND(CAST(subtotal AS NUMERIC) / 100, 2) as subtotal,
    updated_at,
    last_processed_at
from {{ ref('stg_order_event_c7') }}
{% if is_incremental() %}
    where date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
{% endif %} 