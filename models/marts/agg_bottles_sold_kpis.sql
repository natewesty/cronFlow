{{
  config(
    materialized='table'
  )
}}

with daily_bottles_sold as (
    select
        fo.order_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        sum(foi.quantity) as daily_bottles_sold
    from {{ ref('fct_order_item') }} foi
    left join {{ ref('fct_order') }} fo on foi.order_id = fo.order_id
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    where foi.item_type = 'Wine'  -- Only include wine items
    and fo.channel <> 'club'  -- Exclude club channel orders
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

bottles_metrics as (
    select
        -- Today's Bottles Sold
        coalesce((
            select daily_bottles_sold 
            from daily_bottles_sold 
            where order_date_key = current_date
        ), 0) as bottles_sold_today,
        
        -- Week-to-Date Bottles Sold (Monday start)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('week', current_date)::date
            and order_date_key <= current_date
        ), 0) as bottles_sold_week_to_date,
        
        -- Month-to-Date Bottles Sold
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('month', current_date)::date
            and order_date_key <= current_date
        ), 0) as bottles_sold_month_to_date,
        
        -- Fiscal Year-to-Date Bottles Sold
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = current_date
            )
            and order_date_key <= current_date
        ), 0) as bottles_sold_fiscal_year_to_date,
        
        -- Previous Day Bottles Sold
        coalesce((
            select daily_bottles_sold 
            from daily_bottles_sold 
            where order_date_key = current_date - interval '1 day'
        ), 0) as bottles_sold_prev_day,
        
        -- Previous Week Bottles Sold (same week last year)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('week', current_date)::date - interval '1 year'
            and order_date_key <= current_date - interval '1 year'
        ), 0) as bottles_sold_prev_week,
        
        -- Previous Month Bottles Sold (same month last year)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('month', current_date)::date - interval '1 year'
            and order_date_key <= current_date - interval '1 year'
        ), 0) as bottles_sold_prev_month,
        
        -- Previous Fiscal Year Bottles Sold for same period
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = current_date
            ) - 1
            and order_date_key <= current_date
        ), 0) as bottles_sold_prev_fiscal_year_to_date
)

select
    current_date as report_date,
    
    -- Today's metrics
    bottles_sold_today,
    bottles_sold_prev_day,
    bottles_sold_today - bottles_sold_prev_day as bottles_sold_today_vs_prev_day,
    case 
        when bottles_sold_prev_day > 0 
        then ((bottles_sold_today - bottles_sold_prev_day) / bottles_sold_prev_day) * 100 
        else null 
    end as bottles_sold_today_vs_prev_day_pct,
    
    -- Week-to-Date metrics
    bottles_sold_week_to_date,
    bottles_sold_prev_week,
    bottles_sold_week_to_date - bottles_sold_prev_week as bottles_sold_week_vs_prev_week,
    case 
        when bottles_sold_prev_week > 0 
        then ((bottles_sold_week_to_date - bottles_sold_prev_week) / bottles_sold_prev_week) * 100 
        else null 
    end as bottles_sold_week_vs_prev_week_pct,
    
    -- Month-to-Date metrics
    bottles_sold_month_to_date,
    bottles_sold_prev_month,
    bottles_sold_month_to_date - bottles_sold_prev_month as bottles_sold_month_vs_prev_month,
    case 
        when bottles_sold_prev_month > 0 
        then ((bottles_sold_month_to_date - bottles_sold_prev_month) / bottles_sold_prev_month) * 100 
        else null 
    end as bottles_sold_month_vs_prev_month_pct,
    
    -- Fiscal Year-to-Date metrics
    bottles_sold_fiscal_year_to_date,
    bottles_sold_prev_fiscal_year_to_date,
    bottles_sold_fiscal_year_to_date - bottles_sold_prev_fiscal_year_to_date as bottles_sold_fiscal_year_vs_prev_fiscal_year,
    case 
        when bottles_sold_prev_fiscal_year_to_date > 0 
        then ((bottles_sold_fiscal_year_to_date - bottles_sold_prev_fiscal_year_to_date) / bottles_sold_prev_fiscal_year_to_date) * 100 
        else null 
    end as bottles_sold_fiscal_year_vs_prev_fiscal_year_pct,
    
    -- Current fiscal year info
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = current_date) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = current_date - interval '1 year') as previous_fiscal_year

from bottles_metrics 