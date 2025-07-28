{{ config(
    materialized='incremental',
    unique_key='variant_id',
    incremental_strategy='merge'
) }}

with base as (
    select product_id, _product_json as p, updated_at
    from {{ ref('stg_product') }}
),

variant as (
    select
        product_id,
        (v->>'id')::uuid                      as variant_id,
        v->>'title'                           as variant_title,
        v->>'sku'                             as sku,
        v->>'inventoryPolicy'                 as inventory_policy,
        v->>'taxType'                         as tax_type,
        (v->>'hasShipping')::boolean          as has_shipping,
        (v->>'hasInventory')::boolean         as has_inventory,
        (v->>'weight')::numeric               as weight_kg,
        (v->>'volumeInML')::int               as volume_ml,
        (v->>'alcoholPercentage')::numeric    as abv,
        (v->>'costOfGood')::bigint            as cogs_cents,
        (v->>'price')::bigint                 as price_cents,
        (v->>'bottleDeposit')::bigint         as deposit_cents,
        (v->>'comparePrice')::bigint          as compare_price_cents,
        (v->>'maxQuantityPerCart')::int       as max_qty_per_cart,
        (v->>'sortOrder')::int                as sort_order,
        updated_at
    from base
    cross join lateral jsonb_array_elements(p->'variants') v
)

select * from variant

{% if is_incremental() %}
 where updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
        from {{ this }})
{% endif %}
