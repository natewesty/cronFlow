-- models/marts/dim_product.sql
-- Product dimension mart

{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

with latest as (
    select *,
           row_number() over (partition by product_id order by updated_at desc) as rn
    from {{ ref('stg_product') }}
    {% if is_incremental() %}
        where date(last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
)
select
    product_id,
    title,
    created_at,
    updated_at,
    last_processed_at
from latest
where rn = 1 