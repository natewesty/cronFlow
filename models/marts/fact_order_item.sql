-- models/marts/fact_order_item.sql
-- Order item fact mart

{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id', 'product_variant_id'],
    incremental_strategy='merge'
) }}

select
    order_id,
    product_title,
    item_type,
    product_id,
    product_variant_title,
    product_variant_id,
    sku,
    price,
    quantity,
    updated_at,
    last_processed_at
from {{ ref('stg_order_items') }}
{% if is_incremental() %}
    where date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
{% endif %} 