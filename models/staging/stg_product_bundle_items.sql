-- models/staging/stg_product_bundle_items.sql
-- Staging model for Commerce7 product bundle items

{{ config(
    materialized='incremental',
    unique_key='bundled_item_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        id as product_id,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_product') }}
    where jsonb_typeof(data->'bundleItems') = 'array'
    {% if is_incremental() %}
        and date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
),
unnested as (
    select
        base.product_id,
        base.updated_at,
        base.last_processed_at,
        base.data->>'title' as bundle_name,
        item->>'id' as bundled_item_id,
        item->>'productId' as bundled_product_id,
        item->>'productVariantId' as bundled_product_variant_id,
        item->>'productTitle' as bundled_product_title,
        item->>'quantity' as bundled_quantity,
        round((item->>'price')::numeric / 100, 2) as bundled_price,
        item->>'sku' as bundled_sku
    from base
    cross join jsonb_array_elements(base.data->'bundleItems') as item
)
select * from unnested 