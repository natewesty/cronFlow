{{
  config(
    materialized='table'
  )
}}

with daily_signups as (
    select
        date(sm.signup_at) as signup_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        count(*) as daily_signups
    from {{ ref('stg_club_membership') }} sm
    left join {{ ref('dim_date') }} dd on date(sm.signup_at) = dd.date_day
    where sm.signup_at is not null
    group by date(sm.signup_at), dd.fiscal_year, dd.fiscal_year_name
),

daily_cancellations as (
    select
        date(sm.cancel_at) as cancel_date_key,
        dd.fiscal_year,
        dd.fiscal_year_name,
        count(*) as daily_cancellations
    from {{ ref('stg_club_membership') }} sm
    left join {{ ref('dim_date') }} dd on date(sm.cancel_at) = dd.date_day
    where sm.cancel_at is not null
    group by date(sm.cancel_at), dd.fiscal_year, dd.fiscal_year_name
),

daily_net_signups as (
    select
        coalesce(s.signup_date_key, c.cancel_date_key) as date_key,
        coalesce(s.fiscal_year, c.fiscal_year) as fiscal_year,
        coalesce(s.fiscal_year_name, c.fiscal_year_name) as fiscal_year_name,
        coalesce(s.daily_signups, 0) as daily_signups,
        coalesce(c.daily_cancellations, 0) as daily_cancellations,
        coalesce(s.daily_signups, 0) - coalesce(c.daily_cancellations, 0) as daily_net_signups
    from daily_signups s
    full outer join daily_cancellations c on s.signup_date_key = c.cancel_date_key
),

signup_metrics as (
    select
        -- Today's Net Signups (Pacific Time)
        coalesce((
            select daily_net_signups
            from daily_net_signups
            where date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as net_signups_today,
        
        -- Today's Signups (Pacific Time)
        coalesce((
            select daily_signups
            from daily_net_signups
            where date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as signups_today,
        
        -- Today's Cancellations (Pacific Time)
        coalesce((
            select daily_cancellations
            from daily_net_signups
            where date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as cancellations_today,
        
        -- Week-to-Date Net Signups (Monday start, Pacific Time)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as net_signups_week_to_date,
        
        -- Week-to-Date Signups (Pacific Time)
        coalesce((
            select sum(daily_signups)
            from daily_net_signups
            where date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as signups_week_to_date,
        
        -- Week-to-Date Cancellations (Pacific Time)
        coalesce((
            select sum(daily_cancellations)
            from daily_net_signups
            where date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as cancellations_week_to_date,
        
        -- Month-to-Date Net Signups (Pacific Time)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as net_signups_month_to_date,
        
        -- Month-to-Date Signups (Pacific Time)
        coalesce((
            select sum(daily_signups)
            from daily_net_signups
            where date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as signups_month_to_date,
        
        -- Month-to-Date Cancellations (Pacific Time)
        coalesce((
            select sum(daily_cancellations)
            from daily_net_signups
            where date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as cancellations_month_to_date,
        
        -- Fiscal Year-to-Date Net Signups (Pacific Time)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as net_signups_fiscal_year_to_date,
        
        -- Fiscal Year-to-Date Signups (Pacific Time)
        coalesce((
            select sum(daily_signups)
            from daily_net_signups
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as signups_fiscal_year_to_date,
        
        -- Fiscal Year-to-Date Cancellations (Pacific Time)
        coalesce((
            select sum(daily_cancellations)
            from daily_net_signups
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1)
        ), 0) as cancellations_fiscal_year_to_date,
        
        -- Previous Day Net Signups (same date one year prior, Pacific Time)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where date_key = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as net_signups_prev_day,
        
        -- Previous Week Net Signups (same week last year, Pacific Time)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where date_key >= date_trunc('week', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as net_signups_prev_week,
        
        -- Previous Month Net Signups (same month last year, Pacific Time)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where date_key >= date_trunc('month', (select current_date_pacific from {{ ref('dim_date') }} limit 1))::date - interval '1 year'
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as net_signups_prev_month,
        
        -- Previous Fiscal Year Net Signups (from July 1st of previous fiscal year to today's date minus one year)
        coalesce((
            select sum(daily_net_signups)
            from daily_net_signups
            where fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and date_key >= (
                select date_trunc('year', (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') + interval '6 months'
            )
            and date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as net_signups_prev_fiscal_year_to_date
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    
    -- Today's metrics
    net_signups_today,
    signups_today,
    cancellations_today,
    net_signups_prev_day,
    net_signups_today - net_signups_prev_day as net_signups_today_vs_prev_day,
    case 
        when net_signups_prev_day != 0 
        then ((net_signups_today - net_signups_prev_day)::float / abs(net_signups_prev_day)) * 100 
        else null 
    end as net_signups_today_vs_prev_day_pct,
    
    -- Week-to-Date metrics
    net_signups_week_to_date,
    signups_week_to_date,
    cancellations_week_to_date,
    net_signups_prev_week,
    net_signups_week_to_date - net_signups_prev_week as net_signups_week_vs_prev_week,
    case 
        when net_signups_prev_week != 0 
        then ((net_signups_week_to_date - net_signups_prev_week)::float / abs(net_signups_prev_week)) * 100 
        else null 
    end as net_signups_week_vs_prev_week_pct,
    
    -- Month-to-Date metrics
    net_signups_month_to_date,
    signups_month_to_date,
    cancellations_month_to_date,
    net_signups_prev_month,
    net_signups_month_to_date - net_signups_prev_month as net_signups_month_vs_prev_month,
    case 
        when net_signups_prev_month != 0 
        then ((net_signups_month_to_date - net_signups_prev_month)::float / abs(net_signups_prev_month)) * 100 
        else null 
    end as net_signups_month_vs_prev_month_pct,
    
    -- Fiscal Year-to-Date metrics
    net_signups_fiscal_year_to_date,
    signups_fiscal_year_to_date,
    cancellations_fiscal_year_to_date,
    net_signups_prev_fiscal_year_to_date,
    net_signups_fiscal_year_to_date - net_signups_prev_fiscal_year_to_date as net_signups_fiscal_year_vs_prev_fiscal_year,
    case 
        when net_signups_prev_fiscal_year_to_date != 0 
        then ((net_signups_fiscal_year_to_date - net_signups_prev_fiscal_year_to_date)::float / abs(net_signups_prev_fiscal_year_to_date)) * 100 
        else null 
    end as net_signups_fiscal_year_vs_prev_fiscal_year_pct,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from signup_metrics 