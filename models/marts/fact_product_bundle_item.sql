-- models/marts/fact_product_bundle_item.sql
-- Product bundle item fact mart

{{ config(
    materialized='incremental',
    unique_key='bundled_item_id',
    incremental_strategy='merge'
) }}

select
    product_id,
    bundle_name,
    bundled_item_id,
    bundled_product_id,
    bundled_product_variant_id,
    bundled_product_title,
    bundled_quantity,
    bundled_price,
    bundled_sku,
    updated_at,
    last_processed_at
from {{ ref('stg_product_bundle_items') }}
{% if is_incremental() %}
    where date(last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
{% endif %} 