{{ config(materialized='view') }}

select 
    sku,
    paid_date,
    fulfilled_date,
    channel,
    delivery_method,
    quantity,
    extrapolated_price,
    case_size,
    unit_of_measure
from {{ ref('fct_order_item') }}
where external_order_vendor IS NULL
