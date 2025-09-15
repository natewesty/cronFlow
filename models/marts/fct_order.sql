{% set window_start = "date '2000-01-01'" %}

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
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
    o.updated_at
from {{ ref('stg_order') }} o

{% if is_incremental() %}
 where o.updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', {{ window_start }})
        from {{ this }})
{% endif %}
