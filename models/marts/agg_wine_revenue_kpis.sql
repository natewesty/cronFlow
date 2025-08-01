{{
  config(
    materialized='table'
  )
}}

with daily_wine_revenue as (
    select
        fo.order_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        sum(fo.subtotal) as daily_wine_revenue,
        count(*) as order_count
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    where fo.order_date_key is not null  -- Ensure we have a valid date
    and fo.external_order_vendor is null  -- Only wine revenue (no external vendor)
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

wine_revenue_metrics as (
    select
        -- Today's Wine Revenue (Pacific Time)
        coalesce((
            select daily_wine_revenue 
            from daily_wine_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as wine_revenue_today,
        
        -- Today's Order Count (Pacific Time)
        coalesce((
            select order_count 
            from daily_wine_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as orders_today,
        
        -- Week-to-Date Wine Revenue (Monday start, Pacific Time)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as wine_revenue_week_to_date,
        
        -- Month-to-Date Wine Revenue (Pacific Time)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as wine_revenue_month_to_date,
        
        -- Fiscal Year-to-Date Wine Revenue (Pacific Time)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as wine_revenue_fiscal_year_to_date,
        
        -- Previous Fiscal Year Wine Revenue (from July 1st of previous fiscal year to today's date minus one year)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and dr.order_date_key >= (
                select date_trunc('year', (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') + interval '6 months'
            )
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as wine_revenue_prev_fiscal_year_to_date,
        
        -- Previous Week Wine Revenue (same week last year, Pacific Time)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as wine_revenue_prev_week,
        
        -- Previous Month Wine Revenue (same month last year, Pacific Time)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as wine_revenue_prev_month,
        
        -- Previous Day Wine Revenue (same date one year prior, Pacific Time)
        coalesce((
            select sum(daily_wine_revenue) 
            from daily_wine_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as wine_revenue_prev_day,
        
        -- Debug: Total wine orders in fct_order
        (select count(*) from {{ ref('fct_order') }} where payment_status = 'paid' and external_order_vendor is null) as total_wine_orders,
        
        -- Debug: Wine orders with revenue today (Pacific Time)
        (select count(*) from {{ ref('fct_order') }} 
         where payment_status = 'paid' 
         and order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
         and subtotal > 0
         and external_order_vendor is null) as wine_orders_today
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    wine_revenue_today,
    orders_today,
    wine_revenue_prev_day,
    wine_revenue_today - wine_revenue_prev_day as wine_revenue_today_vs_prev_day,
    case 
        when wine_revenue_prev_day > 0 
        then ((wine_revenue_today - wine_revenue_prev_day) / wine_revenue_prev_day) * 100 
        else null 
    end as wine_revenue_today_vs_prev_day_pct,
    
    wine_revenue_week_to_date,
    wine_revenue_prev_week,
    wine_revenue_week_to_date - wine_revenue_prev_week as wine_revenue_week_vs_prev_week,
    case 
        when wine_revenue_prev_week > 0 
        then ((wine_revenue_week_to_date - wine_revenue_prev_week) / wine_revenue_prev_week) * 100 
        else null 
    end as wine_revenue_week_vs_prev_week_pct,
    
    wine_revenue_month_to_date,
    wine_revenue_prev_month,
    wine_revenue_month_to_date - wine_revenue_prev_month as wine_revenue_month_vs_prev_month,
    case 
        when wine_revenue_prev_month > 0 
        then ((wine_revenue_month_to_date - wine_revenue_prev_month) / wine_revenue_prev_month) * 100 
        else null 
    end as wine_revenue_month_vs_prev_month_pct,
    
    wine_revenue_fiscal_year_to_date,
    wine_revenue_prev_fiscal_year_to_date,
    wine_revenue_fiscal_year_to_date - wine_revenue_prev_fiscal_year_to_date as wine_revenue_fiscal_year_vs_prev_fiscal_year,
    case 
        when wine_revenue_prev_fiscal_year_to_date > 0 
        then ((wine_revenue_fiscal_year_to_date - wine_revenue_prev_fiscal_year_to_date) / wine_revenue_prev_fiscal_year_to_date) * 100 
        else null 
    end as wine_revenue_fiscal_year_vs_prev_fiscal_year_pct,
    
    -- Debug fields
    total_wine_orders,
    wine_orders_today,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from wine_revenue_metrics 