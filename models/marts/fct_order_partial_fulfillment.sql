{{ config(
    materialized='table'
) }}

select
    o.order_id,
    o.customer_id,
    o.order_number,
    date(o.paid_at) as order_date_key,
    o.channel,
    o.external_order_vendor,
    o.delivery_method,
    o.payment_status,
    o.fulfillment_status,
    o.sub_total_cents  /100.0  as subtotal,
    o.ship_total_cents /100.0  as shipping,
    o.tax_total_cents  /100.0  as tax,
    o.tip_total_cents  /100.0  as tip,
    o.total_cents      /100.0  as order_total,
    o.total_after_tip_cents /100.0 as total_after_tip,
    o.tasting_lounge,
    o.event_fee_or_wine,
    o.event_specific_sale,
    o.event_revenue_realization_date,
    o.created_at,
    o.updated_at,
    null as tracking,
    null as carrier,
    null as ship_date
from {{ ref('stg_order') }} o

where o.fulfillment_status = 'Partially Fulfilled'