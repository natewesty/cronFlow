{{ config(
    materialized = 'incremental',
    unique_key   = 'md5(customer_id || purchase_at::text || coalesce(product_id::text, sku))',
    incremental_strategy = 'merge'
) }}

with base as (
    select customer_id, _customer_json as c, updated_at
    from {{ ref('stg_customer') }}
),

products as (
    select
        customer_id,

        -- when productId is null (e.g. fees/experiences), keep null so marts can decide how to handle
        (p->'product'->>'productId')::uuid       as product_id,
        p->'product'->>'sku'                     as sku,
        p->'product'->>'title'                   as product_title,
        (p->'product'->>'price')::bigint         as price_cents,
        (p->'product'->>'quantity')::int         as quantity,
        p->'product'->>'image'                   as image_url,

        (p->>'purchaseDate')::timestamptz        as purchase_at,
        p                                         as _product_json,
        updated_at
    from base
    cross join lateral jsonb_array_elements(c->'products') p
)

select * from products

{% if is_incremental() %}
  where updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', '2000‑01‑01')
        from {{ this }}
  )
{% endif %}
