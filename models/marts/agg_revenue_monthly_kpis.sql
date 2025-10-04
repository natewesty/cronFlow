{{
  config(
    materialized='table'
  )
}}

with current_periods as (
    select 
        current_date_pacific,
        fiscal_year as current_fiscal_year,
        fiscal_month as current_fiscal_month
    from {{ ref('dim_date') }} 
    where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
),

monthly_metrics as (
    select
        -- Tasting Room Wine Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) as tasting_room_wine_month_to_date,
        
        -- Tasting Room Wine Month-to-Date Prior: Previous fiscal year same month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= (
                select date_trunc('month', cp.current_date_pacific)::date + interval '1 month' - interval '1 day'
                from current_periods
            ) - interval '1 year'
        ), 0) as tasting_room_wine_month_to_date_prior,
        
        -- Tasting Room Wine Jul: Current fiscal year July
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 1
        ), 0) as tasting_room_wine_jul,
        
        -- Tasting Room Wine Jul Prior: Previous fiscal year July
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 1
        ), 0) as tasting_room_wine_jul_prior,
        
        -- Tasting Room Wine Aug: Current fiscal year August
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 2
        ), 0) as tasting_room_wine_aug,
        
        -- Tasting Room Wine Aug Prior: Previous fiscal year August
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 2
        ), 0) as tasting_room_wine_aug_prior,
        
        -- Tasting Room Wine Sep: Current fiscal year September
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 3
        ), 0) as tasting_room_wine_sep,
        
        -- Tasting Room Wine Sep Prior: Previous fiscal year September
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 3
        ), 0) as tasting_room_wine_sep_prior,
        
        -- Tasting Room Wine Oct: Current fiscal year October
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 4
        ), 0) as tasting_room_wine_oct,
        
        -- Tasting Room Wine Oct Prior: Previous fiscal year October
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 4
        ), 0) as tasting_room_wine_oct_prior,
        
        -- Tasting Room Wine Nov: Current fiscal year November
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 5
        ), 0) as tasting_room_wine_nov,
        
        -- Tasting Room Wine Nov Prior: Previous fiscal year November
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 5
        ), 0) as tasting_room_wine_nov_prior,
        
        -- Tasting Room Wine Dec: Current fiscal year December
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 6
        ), 0) as tasting_room_wine_dec,
        
        -- Tasting Room Wine Dec Prior: Previous fiscal year December
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 6
        ), 0) as tasting_room_wine_dec_prior,
        
        -- Tasting Room Wine Jan: Current fiscal year January
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 7
        ), 0) as tasting_room_wine_jan,
        
        -- Tasting Room Wine Jan Prior: Previous fiscal year January
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 7
        ), 0) as tasting_room_wine_jan_prior,
        
        -- Tasting Room Wine Feb: Current fiscal year February
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 8
        ), 0) as tasting_room_wine_feb,
        
        -- Tasting Room Wine Feb Prior: Previous fiscal year February
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 8
        ), 0) as tasting_room_wine_feb_prior,
        
        -- Tasting Room Wine Mar: Current fiscal year March
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 9
        ), 0) as tasting_room_wine_mar,
        
        -- Tasting Room Wine Mar Prior: Previous fiscal year March
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 9
        ), 0) as tasting_room_wine_mar_prior,
        
        -- Tasting Room Wine Apr: Current fiscal year April
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 10
        ), 0) as tasting_room_wine_apr,
        
        -- Tasting Room Wine Apr Prior: Previous fiscal year April
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 10
        ), 0) as tasting_room_wine_apr_prior,
        
        -- Tasting Room Wine May: Current fiscal year May
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 11
        ), 0) as tasting_room_wine_may,
        
        -- Tasting Room Wine May Prior: Previous fiscal year May
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 11
        ), 0) as tasting_room_wine_may_prior,
        
        -- Tasting Room Wine Jun: Current fiscal year June
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = 12
        ), 0) as tasting_room_wine_jun,
        
        -- Tasting Room Wine Jun Prior: Previous fiscal year June
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_month = 12
        ), 0) as tasting_room_wine_jun_prior
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    
    -- Tasting Room Wine Month-to-Date Metrics
    tasting_room_wine_month_to_date,
    tasting_room_wine_month_to_date_prior,
    tasting_room_wine_month_to_date - tasting_room_wine_month_to_date_prior as tasting_room_wine_month_to_date_variance,
    case 
        when tasting_room_wine_month_to_date_prior > 0 
        then ((tasting_room_wine_month_to_date - tasting_room_wine_month_to_date_prior) / tasting_room_wine_month_to_date_prior) * 100 
        else null 
    end as tasting_room_wine_month_to_date_variance_pct,
    
    -- Tasting Room Wine Monthly Metrics
    tasting_room_wine_jul,
    tasting_room_wine_jul_prior,
    tasting_room_wine_jul - tasting_room_wine_jul_prior as tasting_room_wine_jul_variance,
    case 
        when tasting_room_wine_jul_prior > 0 
        then ((tasting_room_wine_jul - tasting_room_wine_jul_prior) / tasting_room_wine_jul_prior) * 100 
        else null 
    end as tasting_room_wine_jul_variance_pct,
    
    tasting_room_wine_aug,
    tasting_room_wine_aug_prior,
    tasting_room_wine_aug - tasting_room_wine_aug_prior as tasting_room_wine_aug_variance,
    case 
        when tasting_room_wine_aug_prior > 0 
        then ((tasting_room_wine_aug - tasting_room_wine_aug_prior) / tasting_room_wine_aug_prior) * 100 
        else null 
    end as tasting_room_wine_aug_variance_pct,
    
    tasting_room_wine_sep,
    tasting_room_wine_sep_prior,
    tasting_room_wine_sep - tasting_room_wine_sep_prior as tasting_room_wine_sep_variance,
    case 
        when tasting_room_wine_sep_prior > 0 
        then ((tasting_room_wine_sep - tasting_room_wine_sep_prior) / tasting_room_wine_sep_prior) * 100 
        else null 
    end as tasting_room_wine_sep_variance_pct,
    
    tasting_room_wine_oct,
    tasting_room_wine_oct_prior,
    tasting_room_wine_oct - tasting_room_wine_oct_prior as tasting_room_wine_oct_variance,
    case 
        when tasting_room_wine_oct_prior > 0 
        then ((tasting_room_wine_oct - tasting_room_wine_oct_prior) / tasting_room_wine_oct_prior) * 100 
        else null 
    end as tasting_room_wine_oct_variance_pct,
    
    tasting_room_wine_nov,
    tasting_room_wine_nov_prior,
    tasting_room_wine_nov - tasting_room_wine_nov_prior as tasting_room_wine_nov_variance,
    case 
        when tasting_room_wine_nov_prior > 0 
        then ((tasting_room_wine_nov - tasting_room_wine_nov_prior) / tasting_room_wine_nov_prior) * 100 
        else null 
    end as tasting_room_wine_nov_variance_pct,
    
    tasting_room_wine_dec,
    tasting_room_wine_dec_prior,
    tasting_room_wine_dec - tasting_room_wine_dec_prior as tasting_room_wine_dec_variance,
    case 
        when tasting_room_wine_dec_prior > 0 
        then ((tasting_room_wine_dec - tasting_room_wine_dec_prior) / tasting_room_wine_dec_prior) * 100 
        else null 
    end as tasting_room_wine_dec_variance_pct,
    
    tasting_room_wine_jan,
    tasting_room_wine_jan_prior,
    tasting_room_wine_jan - tasting_room_wine_jan_prior as tasting_room_wine_jan_variance,
    case 
        when tasting_room_wine_jan_prior > 0 
        then ((tasting_room_wine_jan - tasting_room_wine_jan_prior) / tasting_room_wine_jan_prior) * 100 
        else null 
    end as tasting_room_wine_jan_variance_pct,
    
    tasting_room_wine_feb,
    tasting_room_wine_feb_prior,
    tasting_room_wine_feb - tasting_room_wine_feb_prior as tasting_room_wine_feb_variance,
    case 
        when tasting_room_wine_feb_prior > 0 
        then ((tasting_room_wine_feb - tasting_room_wine_feb_prior) / tasting_room_wine_feb_prior) * 100 
        else null 
    end as tasting_room_wine_feb_variance_pct,
    
    tasting_room_wine_mar,
    tasting_room_wine_mar_prior,
    tasting_room_wine_mar - tasting_room_wine_mar_prior as tasting_room_wine_mar_variance,
    case 
        when tasting_room_wine_mar_prior > 0 
        then ((tasting_room_wine_mar - tasting_room_wine_mar_prior) / tasting_room_wine_mar_prior) * 100 
        else null 
    end as tasting_room_wine_mar_variance_pct,
    
    tasting_room_wine_apr,
    tasting_room_wine_apr_prior,
    tasting_room_wine_apr - tasting_room_wine_apr_prior as tasting_room_wine_apr_variance,
    case 
        when tasting_room_wine_apr_prior > 0 
        then ((tasting_room_wine_apr - tasting_room_wine_apr_prior) / tasting_room_wine_apr_prior) * 100 
        else null 
    end as tasting_room_wine_apr_variance_pct,
    
    tasting_room_wine_may,
    tasting_room_wine_may_prior,
    tasting_room_wine_may - tasting_room_wine_may_prior as tasting_room_wine_may_variance,
    case 
        when tasting_room_wine_may_prior > 0 
        then ((tasting_room_wine_may - tasting_room_wine_may_prior) / tasting_room_wine_may_prior) * 100 
        else null 
    end as tasting_room_wine_may_variance_pct,
    
    tasting_room_wine_jun,
    tasting_room_wine_jun_prior,
    tasting_room_wine_jun - tasting_room_wine_jun_prior as tasting_room_wine_jun_variance,
    case 
        when tasting_room_wine_jun_prior > 0 
        then ((tasting_room_wine_jun - tasting_room_wine_jun_prior) / tasting_room_wine_jun_prior) * 100 
        else null 
    end as tasting_room_wine_jun_variance_pct,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from monthly_metrics
