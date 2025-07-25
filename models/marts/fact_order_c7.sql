-- models/marts/fact_order_c7.sql
-- Order fact mart for Commerce7 orders

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    order_id,
    customer_id,
    order_paid_date,
    channel,
    subtotal,
    ship_total,
    created_at,
    updated_at,
    last_processed_at
from {{ ref('stg_order') }}
where external_order_vendor is null
{% if is_incremental() %}
    and date(last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
{% endif %} 