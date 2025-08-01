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
    group by fo.order_date_key, dd.fiscal_year, dd.fiscal_year_name
),

bottles_metrics as (
    select
        -- Today's Bottles Sold (Pacific Time)
        coalesce((
            select daily_bottles_sold 
            from daily_bottles_sold 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as bottles_sold_today,
        
        -- Week-to-Date Bottles Sold (Monday start, Pacific Time)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as bottles_sold_week_to_date,
        
        -- Month-to-Date Bottles Sold (Pacific Time)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as bottles_sold_month_to_date,
        
        -- Fiscal Year-to-Date Bottles Sold (Pacific Time)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as bottles_sold_fiscal_year_to_date,
        
        -- Previous Day Bottles Sold (same date one year prior, Pacific Time)
        coalesce((
            select sum(daily_bottles_sold) 
            from daily_bottles_sold 
            where order_date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as bottles_sold_prev_day,
        
        -- Previous Week Bottles Sold (same week last year, Pacific Time)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as bottles_sold_prev_week,
        
        -- Previous Month Bottles Sold (same month last year, Pacific Time)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where order_date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as bottles_sold_prev_month,
        
        -- Previous Fiscal Year Bottles Sold (from July 1st of previous fiscal year to today's date minus one year)
        coalesce((
            select sum(daily_bottles_sold)
            from daily_bottles_sold
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and order_date_key >= (
                select fiscal_year || '-07-01'::date
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - interval '1 year'
            and order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as bottles_sold_prev_fiscal_year_to_date
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    
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
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from bottles_metrics 