{{ config(materialized='table') }}

with current_month as (
    select 
        date_trunc('month', current_date) as month_start,
        date_trunc('month', current_date) + interval '1 month' - interval '1 day' as month_end
),

-- Calculate fiscal year start based on dim_date logic (fiscal year starts July 1)
fiscal_year_period as (
    select 
        case 
            when extract(month from current_date) >= 7 
            then make_date(extract(year from current_date)::int, 7, 1)
            else make_date(extract(year from current_date)::int - 1, 7, 1)
        end as fy_start,
        current_date as fy_end
),

-- Get all unique sales associate names from orders and club memberships for current month
sales_associates as (
    select distinct sales_associate as name
    from {{ ref('stg_order') }}
    where sales_associate is not null
      and paid_at >= (select month_start from current_month)
      and paid_at < (select month_end from current_month)
    
    union
    
    select distinct signup_associate as name
    from {{ ref('stg_club_membership') }}
    where signup_associate is not null
      and signup_at >= (select month_start from current_month)
      and signup_at < (select month_end from current_month)
),

-- Get all unique sales associate names from orders and club memberships for fiscal year to date
sales_associates_fytd as (
    select distinct sales_associate as name
    from {{ ref('stg_order') }}
    where sales_associate is not null
      and paid_at >= (select fy_start from fiscal_year_period)
      and paid_at <= (select fy_end from fiscal_year_period)
    
    union
    
    select distinct signup_associate as name
    from {{ ref('stg_club_membership') }}
    where signup_associate is not null
      and signup_at >= (select fy_start from fiscal_year_period)
      and signup_at <= (select fy_end from fiscal_year_period)
),

-- Revenue and tips from orders
order_metrics as (
    select 
        sales_associate as name,
        sum(sub_total_cents) / 100.0 as revenue,
        sum(tip_total_cents) / 100.0 as tips,
        avg(sub_total_cents) / 100.0 as aov
    from {{ ref('stg_order') }}
    where sales_associate is not null
      and paid_at >= (select month_start from current_month)
      and paid_at < (select month_end from current_month)
    group by sales_associate
),

-- Bottles sold from order items
bottle_metrics as (
    select 
        oi.sales_associate as name,
        sum(oi.qty) as bottles
    from {{ ref('stg_order_item') }} oi
    where oi.sales_associate is not null
      and oi.item_type = 'Wine'
      and oi.paid_at >= (select month_start from current_month)
      and oi.paid_at < (select month_end from current_month)
    group by oi.sales_associate
),

-- Club signups from club memberships
club_signup_metrics as (
    select 
        signup_associate as name,
        count(*) as club_signups
    from {{ ref('stg_club_membership') }}
    where signup_associate is not null
      and signup_at >= (select month_start from current_month)
      and signup_at < (select month_end from current_month)
      and cancel_at is null
    group by signup_associate
),

-- FISCAL YEAR TO DATE METRICS

-- Revenue and tips from orders (FYTD)
order_metrics_fytd as (
    select 
        sales_associate as name,
        sum(sub_total_cents) / 100.0 as revenue_fytd,
        sum(tip_total_cents) / 100.0 as tips_fytd,
        avg(sub_total_cents) / 100.0 as aov_fytd
    from {{ ref('stg_order') }}
    where sales_associate is not null
      and paid_at >= (select fy_start from fiscal_year_period)
      and paid_at <= (select fy_end from fiscal_year_period)
    group by sales_associate
),

-- Bottles sold from order items (FYTD)
bottle_metrics_fytd as (
    select 
        oi.sales_associate as name,
        sum(oi.qty) as bottles_fytd
    from {{ ref('stg_order_item') }} oi
    where oi.sales_associate is not null
      and oi.item_type = 'Wine'
      and oi.paid_at >= (select fy_start from fiscal_year_period)
      and oi.paid_at <= (select fy_end from fiscal_year_period)
    group by oi.sales_associate
),

-- Club signups from club memberships (FYTD)
club_signup_metrics_fytd as (
    select 
        signup_associate as name,
        count(*) as club_signups_fytd
    from {{ ref('stg_club_membership') }}
    where signup_associate is not null
      and signup_at >= (select fy_start from fiscal_year_period)
      and signup_at <= (select fy_end from fiscal_year_period)
      and cancel_at is null
    group by signup_associate
),

-- Combine all sales associates from both monthly and FYTD periods
all_sales_associates as (
    select name from sales_associates
    union
    select name from sales_associates_fytd
)

select 
    sa.name,
    -- Monthly metrics
    coalesce(om.revenue, 0) as revenue,
    coalesce(om.tips, 0) as tips,
    coalesce(bm.bottles, 0) as bottles,
    coalesce(om.aov, 0) as aov,
    coalesce(csm.club_signups, 0) as club_signups,
    -- Fiscal year to date metrics
    coalesce(omf.revenue_fytd, 0) as revenue_fytd,
    coalesce(omf.tips_fytd, 0) as tips_fytd,
    coalesce(bmf.bottles_fytd, 0) as bottles_fytd,
    coalesce(omf.aov_fytd, 0) as aov_fytd,
    coalesce(csmf.club_signups_fytd, 0) as club_signups_fytd
from all_sales_associates sa
left join order_metrics om on sa.name = om.name
left join bottle_metrics bm on sa.name = bm.name
left join club_signup_metrics csm on sa.name = csm.name
left join order_metrics_fytd omf on sa.name = omf.name
left join bottle_metrics_fytd bmf on sa.name = bmf.name
left join club_signup_metrics_fytd csmf on sa.name = csmf.name
order by revenue desc, club_signups desc
