{{ config(materialized='table') }}

with current_month as (
    select 
        date_trunc('month', current_date) as month_start,
        date_trunc('month', current_date) + interval '1 month' - interval '1 day' as month_end
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
)

select 
    sa.name,
    coalesce(om.revenue, 0) as revenue,
    coalesce(om.tips, 0) as tips,
    coalesce(bm.bottles, 0) as bottles,
    coalesce(om.aov, 0) as aov,
    coalesce(csm.club_signups, 0) as club_signups
from sales_associates sa
left join order_metrics om on sa.name = om.name
left join bottle_metrics bm on sa.name = bm.name
left join club_signup_metrics csm on sa.name = csm.name
order by revenue desc, club_signups desc
