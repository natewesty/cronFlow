-- models/staging/stg_order_items.sql
-- Staging model for Commerce7 order items

{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id', 'product_variant_id'],
    incremental_strategy='merge'
) }}

with base as (
    select
        id as order_id,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_order') }}
    where jsonb_typeof(data->'items') = 'array'
    {% if is_incremental() %}
        and date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
),
unnested as (
    select
        base.order_id,
        base.updated_at,
        base.last_processed_at,
        item->>'productTitle' as product_title,
        item->>'type' as item_type,
        item->>'productId' as product_id,
        item->>'productVariantTitle' as product_variant_title,
        item->>'productVariantId' as product_variant_id,
        item->>'sku' as sku,
        round((item->>'price')::numeric / 100, 2) as price,
        item->>'quantity' as quantity
    from base
    cross join jsonb_array_elements(base.data->'items') as item
)
select * from unnested 