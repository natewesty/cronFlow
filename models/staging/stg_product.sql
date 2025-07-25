-- models/staging/stg_product.sql
-- Staging model for Commerce7 product data

{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        data->>'id' as product_id,
        data->>'title' as title,
        data->>'type' as type,
        data->>'webStatus' as web_status,
        data->>'adminStatus' as admin_status,
        data->>'createdAt' as created_at,
        data->>'updatedAt' as updated_at,
        last_processed_at,
        data
    from {{ source('raw', 'raw_product') }}
    {% if is_incremental() %}
        where date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
)

select * from base 