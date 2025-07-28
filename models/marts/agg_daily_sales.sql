-- agg_daily_sales.sql
{{ config(materialized='view') }}

select
    order_date_key,
    sum(order_total)     as daily_sales,
    sum(subtotal)        as daily_subtotal,
    count(distinct order_id) as order_count
from {{ ref('fct_order') }}
group by 1
