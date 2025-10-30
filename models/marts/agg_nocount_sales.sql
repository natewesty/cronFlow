{{ config(materialized='table') }}

with fiscal_year_period as (
    -- Calculate current fiscal year start (FY starts July 1st)
    select 
        case 
            when extract(month from current_date) >= 7 
            then make_date(extract(year from current_date)::int, 7, 1)
            else make_date(extract(year from current_date)::int - 1, 7, 1)
        end as fy_start,
        current_date as fy_end
),

order_items_with_customer as (
    select 
        oi.order_item_id,
        oi.order_id,
        oi.product_title,
        oi.sku,
        oi.qty as quantity,
        oi.paid_at,
        o.customer_id
    from {{ ref('stg_order_item') }} oi
    inner join {{ ref('stg_order') }} o 
        on oi.order_id = o.order_id
    where oi.paid_at >= (select fy_start from fiscal_year_period)
      and oi.paid_at <= (select fy_end from fiscal_year_period)
),

customer_emails as (
    select 
        customer_id,
        first_name,
        last_name,
        primary_email as email
    from {{ ref('dim_customer') }}
    where primary_email like '%@nocount.com'
),

nocount_sales as (
    select
        ce.email,
        ce.first_name,
        ce.last_name,
        oiwc.product_title,
        oiwc.sku,
        sum(oiwc.quantity) as total_quantity_sold
    from order_items_with_customer oiwc
    inner join customer_emails ce 
        on oiwc.customer_id = ce.customer_id
    group by 1, 2, 3, 4, 5
)

select * from nocount_sales
order by email, product_title

