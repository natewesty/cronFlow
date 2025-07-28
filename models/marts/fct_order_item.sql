{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge'
) }}

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.product_variant_id,
    oi.purchase_type,
    oi.item_type,
    oi.item_price,
    oi.item_tax,
    oi.qty                as quantity,      -- ✅ correct column name
    oi.bottle_deposit,
    oi.updated_at
from {{ ref('stg_order_item') }} oi

{% if is_incremental() %}
 where oi.updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
        from {{ this }})
{% endif %}
