{{ config(materialized='incremental',
          unique_key='order_item_id',
          incremental_strategy='merge') }}

with base as (
  select order_id, _order_json as o, updated_at
  from {{ ref('stg_order') }}
),
items as (
  select
    b.order_id,
    (i->>'id')::uuid                as order_item_id,
    i->>'externalOrderVendor'       as external_order_vendor,
    i->>'purchaseType'              as purchase_type,
    i->>'type'                      as item_type,
    i->>'productTitle'              as product_title,
    i->>'productSlug'               as product_slug,
    (i->>'productId')::uuid         as product_id,
    i->>'productVariantTitle'       as variant_title,
    (i->>'productVariantId')::uuid  as variant_id,
    i->>'sku'                       as sku,
    coalesce((i->>'price')::bigint,0)        as price_cents,
    coalesce((i->>'comparePrice')::bigint,0) as compare_price_cents,
    coalesce((i->>'originalPrice')::bigint,0)as original_price_cents,
    coalesce((i->>'costOfGood')::bigint,0)   as cogs_cents,
    coalesce((i->>'bottleDeposit')::bigint,0)as bottle_deposit_cents,
    coalesce((i->>'quantity')::int,0)        as qty,
    coalesce((i->>'tax')::bigint,0)          as tax_cents,
    i->>'taxType'                   as tax_type,
    b.updated_at
  from base b
  cross join lateral jsonb_array_elements(b.o->'items') as i
)
select * from items

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at) - interval '3 days','2000-01-01') from {{ this }})
{% endif %}
