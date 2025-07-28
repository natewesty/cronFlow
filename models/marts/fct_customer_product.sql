{{ config(materialized='table') }}

select
    customer_id,
    product_id,
    sku,
    product_title,
    quantity,
    price_cents/100.0              as price_each,
    purchase_at                    as purchase_ts,
    date(purchase_at)              as purchase_date_key
from {{ ref('stg_customer_product') }};
