-- models/staging/stg_product_variants.sql
-- Staging model for Commerce7 product variants

{{ config(
    materialized='incremental',
    unique_key='variant_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        id as product_id,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_product') }}
    where jsonb_typeof(data->'variants') = 'array'
    {% if is_incremental() %}
        and date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
),
unnested as (
    select
        base.product_id,
        base.updated_at,
        base.last_processed_at,
        base.data->>'title' as product_title,
        item->>'id' as variant_id,
        item->>'title' as variant_title,
        item->>'sku' as variant_sku,
        item->>'volumeInML' as variant_volume_ml,
        item->>'alcoholPercentage' as variant_alcohol_pct,
        round((item->>'price')::numeric / 100, 2) as variant_price,
        item->>'weight' as variant_weight
    from base
    cross join jsonb_array_elements(base.data->'variants') as item
)
select * from unnested 