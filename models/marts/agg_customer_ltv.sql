-- agg_customer_ltv.sql
{{ config(materialized='view') }}

select
    c.customer_id,
    count(distinct o.order_id)            as orders,
    sum(o.order_total)                    as lifetime_sales,
    max(o.order_date_key)                 as most_recent_order_date
from {{ ref('dim_customer') }}    c
left join {{ ref('fct_order') }}  o using (customer_id)
group by 1;
