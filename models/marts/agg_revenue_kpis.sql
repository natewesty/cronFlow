{{
  config(
    materialized='table'
  )
}}

with daily_tasting_revenue as (
    select
        fo.order_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        sum(fo.subtotal) as daily_tasting_revenue,
        count(*) as tasting_order_count
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    where fo.order_date_key is not null  -- Ensure we have a valid date
    and fo.external_order_vendor = 'Tock'  -- Tasting fee revenue
    and fo.payment_status = 'paid'
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

daily_wine_revenue as (
    select
        fo.order_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        sum(fo.subtotal) as daily_wine_revenue,
        count(*) as wine_order_count
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    where fo.order_date_key is not null  -- Ensure we have a valid date
    and fo.external_order_vendor is null  -- Wine revenue
    and fo.payment_status = 'paid'
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

tasting_revenue_metrics as (
    select
        -- Today's Tasting Revenue (Pacific Time)
        coalesce((
            select daily_tasting_revenue 
            from daily_tasting_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_revenue_today,
        
        -- Today's Tasting Order Count (Pacific Time)
        coalesce((
            select tasting_order_count 
            from daily_tasting_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_orders_today,
        
        -- Week-to-Date Tasting Revenue (Monday start, Pacific Time)
        coalesce((
            select sum(daily_tasting_revenue)
            from daily_tasting_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_revenue_week_to_date,
        
        -- Month-to-Date Tasting Revenue (Pacific Time)
        coalesce((
            select sum(daily_tasting_revenue)
            from daily_tasting_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_revenue_month_to_date,
        
        -- Fiscal Year-to-Date Tasting Revenue (Pacific Time)
        coalesce((
            select sum(daily_tasting_revenue)
            from daily_tasting_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_revenue_fiscal_year_to_date,
        
        -- Previous Fiscal Year Tasting Revenue for same period (Pacific Time)
        coalesce((
            select sum(daily_tasting_revenue)
            from daily_tasting_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_revenue_prev_fiscal_year_to_date,
        
        -- Previous Week Tasting Revenue (same week last year, Pacific Time)
        coalesce((
            select sum(daily_tasting_revenue)
            from daily_tasting_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_revenue_prev_week,
        
        -- Previous Month Tasting Revenue (same month last year, Pacific Time)
        coalesce((
            select sum(daily_tasting_revenue)
            from daily_tasting_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_revenue_prev_month,
        
        -- Previous Day Tasting Revenue (Pacific Time)
        coalesce((
            select daily_tasting_revenue 
            from daily_tasting_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 day'
        ), 0) as tasting_revenue_prev_day
),

wine_revenue_metrics as (
    select
        -- Today's Wine Revenue (Pacific Time)
        coalesce((
            select daily_wine_revenue 
            from daily_wine_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as wine_revenue_today,
        
        -- Today's Wine Order Count (Pacific Time)
        coalesce((
            select wine_order_count 
            from daily_wine_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as wine_orders_today,
        
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
        
        -- Previous Fiscal Year Wine Revenue for same period (Pacific Time)
        coalesce((
            select sum(daily_wine_revenue)
            from daily_wine_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
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
        
        -- Previous Day Wine Revenue (Pacific Time)
        coalesce((
            select daily_wine_revenue 
            from daily_wine_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 day'
        ), 0) as wine_revenue_prev_day
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    
    -- Tasting Fee Revenue Metrics
    tasting_revenue_today,
    tasting_orders_today,
    tasting_revenue_prev_day,
    tasting_revenue_today - tasting_revenue_prev_day as tasting_revenue_today_vs_prev_day,
    case 
        when tasting_revenue_prev_day > 0 
        then ((tasting_revenue_today - tasting_revenue_prev_day) / tasting_revenue_prev_day) * 100 
        else null 
    end as tasting_revenue_today_vs_prev_day_pct,
    
    tasting_revenue_week_to_date,
    tasting_revenue_prev_week,
    tasting_revenue_week_to_date - tasting_revenue_prev_week as tasting_revenue_week_vs_prev_week,
    case 
        when tasting_revenue_prev_week > 0 
        then ((tasting_revenue_week_to_date - tasting_revenue_prev_week) / tasting_revenue_prev_week) * 100 
        else null 
    end as tasting_revenue_week_vs_prev_week_pct,
    
    tasting_revenue_month_to_date,
    tasting_revenue_prev_month,
    tasting_revenue_month_to_date - tasting_revenue_prev_month as tasting_revenue_month_vs_prev_month,
    case 
        when tasting_revenue_prev_month > 0 
        then ((tasting_revenue_month_to_date - tasting_revenue_prev_month) / tasting_revenue_prev_month) * 100 
        else null 
    end as tasting_revenue_month_vs_prev_month_pct,
    
    tasting_revenue_fiscal_year_to_date,
    tasting_revenue_prev_fiscal_year_to_date,
    tasting_revenue_fiscal_year_to_date - tasting_revenue_prev_fiscal_year_to_date as tasting_revenue_fiscal_year_vs_prev_fiscal_year,
    case 
        when tasting_revenue_prev_fiscal_year_to_date > 0 
        then ((tasting_revenue_fiscal_year_to_date - tasting_revenue_prev_fiscal_year_to_date) / tasting_revenue_prev_fiscal_year_to_date) * 100 
        else null 
    end as tasting_revenue_fiscal_year_vs_prev_fiscal_year_pct,
    
    -- Wine Revenue Metrics
    wine_revenue_today,
    wine_orders_today,
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
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from tasting_revenue_metrics, wine_revenue_metrics 