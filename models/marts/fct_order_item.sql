{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge'
) }}

with joined_data as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.customer_id,
        oi.product_id,
        oi.product_title,
        oi.external_order_vendor,
        oi.variant_id       as product_variant_id,   -- ✅ column exists in stg_order_item
        oi.purchase_type,
        oi.item_type,
        oi.sku,                                     -- ✅ Added from stg_order_item
        oi.channel,                                    -- ✅ Added from stg_order_item
        oi.paid_at,                                   -- ✅ Added from stg_order_item
        date(oi.paid_at) as paid_date,                -- Date format for RFM calculations
        oi.fulfilled_at,
        date(oi.fulfilled_at) as fulfilled_date,
        oi.delivery_method,
        oi.price_cents      / 100.0    as item_price,
        oi.tax_cents        / 100.0    as item_tax,
        oi.qty              as quantity,             -- ✅ matches stg_order_item
        (oi.price_cents / 100.0) * oi.qty as product_subtotal,
        oi.bottle_deposit_cents / 100.0    as bottle_deposit,
        pv.extrapolated_price,
        pv.case_size,
        pv.unit_of_measure,
        oi.updated_at,
        row_number() over (partition by oi.order_item_id order by oi.updated_at desc) as rn
    from {{ ref('stg_order_item') }} oi
    left join {{ ref('dim_product_variant') }} pv
        on oi.sku = pv.sku

    {% if is_incremental() %}
     where oi.updated_at >= (
            select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
            from {{ this }})
    {% endif %}
)
select
    order_item_id,
    order_id,
    customer_id,
    product_id,
    product_title,
    external_order_vendor,
    product_variant_id,
    purchase_type,
    item_type,
    sku,
    channel,
    paid_at,
    paid_date,
    fulfilled_at,
    fulfilled_date,
    delivery_method,
    item_price,
    item_tax,
    quantity,
    product_subtotal,
    bottle_deposit,
    extrapolated_price,
    case_size,
    unit_of_measure,
    updated_at
from joined_data
where rn = 1
