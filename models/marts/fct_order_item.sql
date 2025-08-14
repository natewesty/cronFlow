{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge'
) }}

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.external_order_vendor,
    oi.variant_id       as product_variant_id,   -- ✅ column exists in stg_order_item
    oi.purchase_type,
    oi.item_type,
    oi.price_cents      / 100.0    as item_price,
    oi.tax_cents        / 100.0    as item_tax,
    oi.qty              as quantity,             -- ✅ matches stg_order_item
    oi.bottle_deposit_cents / 100.0    as bottle_deposit,
    oi.channel,
    oi.paid_at,
    oi.updated_at
from {{ ref('stg_order_item') }} oi

{% if is_incremental() %}
 where oi.updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
        from {{ this }})
{% endif %}
