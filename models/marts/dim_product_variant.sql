-- models/marts/dim_product_variant.sql
-- Product variant dimension mart

{{ config(
    materialized='incremental',
    unique_key='variant_id',
    incremental_strategy='merge'
) }}

select
    product_id,
    product_title,
    variant_id,
    variant_title,
    variant_sku,
    variant_volume_ml,
    variant_alcohol_pct,
    variant_price,
    variant_weight,
    updated_at,
    last_processed_at
from {{ ref('stg_product_variants') }}
{% if is_incremental() %}
    where date(last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
{% endif %} 