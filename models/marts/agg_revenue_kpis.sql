{{
  config(
    materialized='table'
  )
}}

with daily_revenue as (
    select
        fo.order_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        sum(fo.subtotal) as daily_revenue,
        count(*) as order_count
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    where fo.order_date_key is not null  -- Ensure we have a valid date
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

revenue_metrics as (
    select
        -- Today's Revenue (Pacific Time)
        coalesce((
            select daily_revenue 
            from daily_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as revenue_today,
        
        -- Today's Order Count (Pacific Time)
        coalesce((
            select order_count 
            from daily_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as orders_today,
        
        -- Week-to-Date Revenue (Monday start, Pacific Time)
        coalesce((
            select sum(daily_revenue)
            from daily_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as revenue_week_to_date,
        
        -- Month-to-Date Revenue (Pacific Time)
        coalesce((
            select sum(daily_revenue)
            from daily_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as revenue_month_to_date,
        
        -- Fiscal Year-to-Date Revenue (Pacific Time)
        coalesce((
            select sum(daily_revenue)
            from daily_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as revenue_fiscal_year_to_date,
        
        -- Previous Fiscal Year Revenue for same period (Pacific Time)
        coalesce((
            select sum(daily_revenue)
            from daily_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as revenue_prev_fiscal_year_to_date,
        
        -- Previous Week Revenue (same week last year, Pacific Time)
        coalesce((
            select sum(daily_revenue)
            from daily_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as revenue_prev_week,
        
        -- Previous Month Revenue (same month last year, Pacific Time)
        coalesce((
            select sum(daily_revenue)
            from daily_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as revenue_prev_month,
        
        -- Previous Day Revenue (Pacific Time)
        coalesce((
            select daily_revenue 
            from daily_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 day'
        ), 0) as revenue_prev_day,
        
        -- Debug: Total orders in fct_order (excluding club)
        (select count(*) from {{ ref('fct_order') }} where payment_status = 'paid' and channel <> 'club') as total_paid_orders,
        
        -- Debug: Orders with revenue today (excluding club, Pacific Time)
        (select count(*) from {{ ref('fct_order') }} 
         where payment_status = 'paid' 
         and order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
         and subtotal > 0
         and channel <> 'club') as paid_orders_today
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    revenue_today,
    orders_today,
    revenue_prev_day,
    revenue_today - revenue_prev_day as revenue_today_vs_prev_day,
    case 
        when revenue_prev_day > 0 
        then ((revenue_today - revenue_prev_day) / revenue_prev_day) * 100 
        else null 
    end as revenue_today_vs_prev_day_pct,
    
    revenue_week_to_date,
    revenue_prev_week,
    revenue_week_to_date - revenue_prev_week as revenue_week_vs_prev_week,
    case 
        when revenue_prev_week > 0 
        then ((revenue_week_to_date - revenue_prev_week) / revenue_prev_week) * 100 
        else null 
    end as revenue_week_vs_prev_week_pct,
    
    revenue_month_to_date,
    revenue_prev_month,
    revenue_month_to_date - revenue_prev_month as revenue_month_vs_prev_month,
    case 
        when revenue_prev_month > 0 
        then ((revenue_month_to_date - revenue_prev_month) / revenue_prev_month) * 100 
        else null 
    end as revenue_month_vs_prev_month_pct,
    
    revenue_fiscal_year_to_date,
    revenue_prev_fiscal_year_to_date,
    revenue_fiscal_year_to_date - revenue_prev_fiscal_year_to_date as revenue_fiscal_year_vs_prev_fiscal_year,
    case 
        when revenue_prev_fiscal_year_to_date > 0 
        then ((revenue_fiscal_year_to_date - revenue_prev_fiscal_year_to_date) / revenue_prev_fiscal_year_to_date) * 100 
        else null 
    end as revenue_fiscal_year_vs_prev_fiscal_year_pct,
    
    -- Debug fields
    total_paid_orders,
    paid_orders_today,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from revenue_metrics 