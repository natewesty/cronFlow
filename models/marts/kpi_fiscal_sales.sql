-- models/marts/kpi_fiscal_sales.sql
-- KPI mart for fiscal year sales metrics

{{ config(
    materialized='incremental',
    unique_key='fiscal_year',
    incremental_strategy='merge'
) }}

with fiscal_sales as (
    select
        fd.fiscal_year,
        fd.fiscal_year_label,
        fd.fiscal_year_start_date,
        fd.fiscal_year_end_date,
        count(distinct fo.order_id) as total_orders,
        sum(fo.subtotal) as total_revenue,
        sum(fo.ship_total) as total_shipping,
        sum(fo.subtotal + fo.ship_total) as total_gross_revenue,
        count(distinct fo.customer_id) as unique_customers,
        round(avg(fo.subtotal), 2) as average_order_value,
        round(sum(fo.subtotal) / nullif(count(distinct fo.customer_id), 0), 2) as revenue_per_customer,
        max(fo.updated_at) as updated_at,
        max(fo.last_processed_at) as last_processed_at
    from {{ ref('fact_order_c7') }} fo
    join {{ ref('dim_fiscal_date') }} fd
        on date(fo.order_paid_date) = fd.calendar_date
    {% if is_incremental() %}
        where date(fo.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        fd.fiscal_year,
        fd.fiscal_year_label,
        fd.fiscal_year_start_date,
        fd.fiscal_year_end_date
    order by fd.fiscal_year
)

select * from fiscal_sales 