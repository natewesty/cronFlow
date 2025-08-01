{{
  config(
    materialized='table'
  )
}}

with daily_tasting_fee_revenue as (
    select
        fo.order_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        sum(fo.subtotal) as daily_tasting_fee_revenue,
        count(*) as order_count
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    where fo.order_date_key is not null  -- Ensure we have a valid date
    and fo.external_order_vendor = 'Tock'  -- Only tasting fee revenue
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

tasting_fee_revenue_metrics as (
    select
        -- Today's Tasting Fee Revenue (Pacific Time)
        coalesce((
            select daily_tasting_fee_revenue 
            from daily_tasting_fee_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_fee_revenue_today,
        
        -- Today's Order Count (Pacific Time)
        coalesce((
            select order_count 
            from daily_tasting_fee_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as orders_today,
        
        -- Week-to-Date Tasting Fee Revenue (Monday start, Pacific Time)
        coalesce((
            select sum(daily_tasting_fee_revenue)
            from daily_tasting_fee_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_fee_revenue_week_to_date,
        
        -- Month-to-Date Tasting Fee Revenue (Pacific Time)
        coalesce((
            select sum(daily_tasting_fee_revenue)
            from daily_tasting_fee_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_fee_revenue_month_to_date,
        
        -- Fiscal Year-to-Date Tasting Fee Revenue (Pacific Time)
        coalesce((
            select sum(daily_tasting_fee_revenue)
            from daily_tasting_fee_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as tasting_fee_revenue_fiscal_year_to_date,
        
        -- Previous Fiscal Year Tasting Fee Revenue (from July 1st of previous fiscal year to today's date minus one year)
        coalesce((
            select sum(daily_tasting_fee_revenue)
            from daily_tasting_fee_revenue dr
            where dr.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and dr.order_date_key >= (
                select fiscal_year || '-07-01'::date
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_fee_revenue_prev_fiscal_year_to_date,
        
        -- Previous Week Tasting Fee Revenue (same week last year, Pacific Time)
        coalesce((
            select sum(daily_tasting_fee_revenue)
            from daily_tasting_fee_revenue dr
            where dr.order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_fee_revenue_prev_week,
        
        -- Previous Month Tasting Fee Revenue (same month last year, Pacific Time)
        coalesce((
            select sum(daily_tasting_fee_revenue)
            from daily_tasting_fee_revenue dr
            where dr.order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and dr.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_fee_revenue_prev_month,
        
        -- Previous Day Tasting Fee Revenue (same date one year prior, Pacific Time)
        coalesce((
            select sum(daily_tasting_fee_revenue) 
            from daily_tasting_fee_revenue 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_fee_revenue_prev_day,
        
        -- Debug: Total tasting fee orders in fct_order
        (select count(*) from {{ ref('fct_order') }} where payment_status = 'paid' and external_order_vendor = 'Tock') as total_tasting_fee_orders,
        
        -- Debug: Tasting fee orders with revenue today (Pacific Time)
        (select count(*) from {{ ref('fct_order') }} 
         where payment_status = 'paid' 
         and order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
         and subtotal > 0
         and external_order_vendor = 'Tock') as tasting_fee_orders_today
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    tasting_fee_revenue_today,
    orders_today,
    tasting_fee_revenue_prev_day,
    tasting_fee_revenue_today - tasting_fee_revenue_prev_day as tasting_fee_revenue_today_vs_prev_day,
    case 
        when tasting_fee_revenue_prev_day > 0 
        then ((tasting_fee_revenue_today - tasting_fee_revenue_prev_day) / tasting_fee_revenue_prev_day) * 100 
        else null 
    end as tasting_fee_revenue_today_vs_prev_day_pct,
    
    tasting_fee_revenue_week_to_date,
    tasting_fee_revenue_prev_week,
    tasting_fee_revenue_week_to_date - tasting_fee_revenue_prev_week as tasting_fee_revenue_week_vs_prev_week,
    case 
        when tasting_fee_revenue_prev_week > 0 
        then ((tasting_fee_revenue_week_to_date - tasting_fee_revenue_prev_week) / tasting_fee_revenue_prev_week) * 100 
        else null 
    end as tasting_fee_revenue_week_vs_prev_week_pct,
    
    tasting_fee_revenue_month_to_date,
    tasting_fee_revenue_prev_month,
    tasting_fee_revenue_month_to_date - tasting_fee_revenue_prev_month as tasting_fee_revenue_month_vs_prev_month,
    case 
        when tasting_fee_revenue_prev_month > 0 
        then ((tasting_fee_revenue_month_to_date - tasting_fee_revenue_prev_month) / tasting_fee_revenue_prev_month) * 100 
        else null 
    end as tasting_fee_revenue_month_vs_prev_month_pct,
    
    tasting_fee_revenue_fiscal_year_to_date,
    tasting_fee_revenue_prev_fiscal_year_to_date,
    tasting_fee_revenue_fiscal_year_to_date - tasting_fee_revenue_prev_fiscal_year_to_date as tasting_fee_revenue_fiscal_year_vs_prev_fiscal_year,
    case 
        when tasting_fee_revenue_prev_fiscal_year_to_date > 0 
        then ((tasting_fee_revenue_fiscal_year_to_date - tasting_fee_revenue_prev_fiscal_year_to_date) / tasting_fee_revenue_prev_fiscal_year_to_date) * 100 
        else null 
    end as tasting_fee_revenue_fiscal_year_vs_prev_fiscal_year_pct,
    
    -- Debug fields
    total_tasting_fee_orders,
    tasting_fee_orders_today,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from tasting_fee_revenue_metrics 