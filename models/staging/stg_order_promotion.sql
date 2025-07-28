{{ config(
    materialized='incremental',
    unique_key='order_id||promotion_row_id',
    incremental_strategy='merge'
) }}

with base as (
    select order_id, _order_json as o, updated_at
    from {{ ref('stg_order') }}
),

promos as (
    select
        order_id,
        (p->>'id')::uuid                as promotion_row_id,
        (p->>'promotionId')::uuid       as promotion_id,
        p->>'title'                     as promotion_title,
        coalesce((p->>'productValue')::bigint,0)   as product_value_cents,
        coalesce((p->>'shippingValue')::bigint,0)  as shipping_value_cents,
        coalesce((p->>'totalValue')::bigint,0)     as total_value_cents,
        updated_at
    from base
    cross join lateral jsonb_array_elements(o->'promotions') p
)

select * from promos

{% if is_incremental() %}
  where updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
        from {{ this }})
{% endif %}
