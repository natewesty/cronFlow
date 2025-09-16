{{
  config(
    materialized='table'
  )
}}

with tasting_room_metrics as (
    select
        -- Tasting Room Wine Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as tasting_room_wine_actual,
        
        -- Tasting Room Wine Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'POS'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_room_wine_prior,
        
        -- Tasting Room Fees Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'POS'
            and fo.external_order_vendor = 'Tock'
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as tasting_room_fees_actual,
        
        -- Tasting Room Fees Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'POS'
            and fo.external_order_vendor = 'Tock'
            and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
            and fo.event_fee_or_wine is null
            and fo.event_specific_sale is null
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_room_fees_prior,
        
        -- Wine Club Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'Club'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as wine_club_actual,
        
        -- Wine Club Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'Club'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as wine_club_prior,
        
        -- eComm Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'Web'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as ecomm_actual,
        
        -- eComm Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'Web'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as ecomm_prior,
        
        -- Phone Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'Inbound'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as phone_actual,
        
        -- Phone Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.channel = 'Inbound'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as phone_prior,
        
        -- Event Fees Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = '1'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as event_fees_actual,
        
        -- Event Fees Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = '1'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as event_fees_prior,
        
        -- Event Wine Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = '1'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as event_wine_actual,
        
        -- Event Wine Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = '1'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as event_wine_prior,
        
        -- Shipping Actual: Current fiscal year
        coalesce((
            select sum(fo.shipping)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            )
        ), 0) as shipping_actual,
        
        -- Shipping Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.shipping)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and fo.order_date_key >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and fo.order_date_key <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as shipping_prior
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    tasting_room_wine_actual,
    tasting_room_wine_prior,
    tasting_room_wine_actual - tasting_room_wine_prior as tasting_room_wine_variance,
    case 
        when tasting_room_wine_prior > 0 
        then ((tasting_room_wine_actual - tasting_room_wine_prior) / tasting_room_wine_prior) * 100 
        else null 
    end as tasting_room_wine_variance_pct,
    
    tasting_room_fees_actual,
    tasting_room_fees_prior,
    tasting_room_fees_actual - tasting_room_fees_prior as tasting_room_fees_variance,
    case 
        when tasting_room_fees_prior > 0 
        then ((tasting_room_fees_actual - tasting_room_fees_prior) / tasting_room_fees_prior) * 100 
        else null 
    end as tasting_room_fees_variance_pct,
    
    wine_club_actual,
    wine_club_prior,
    wine_club_actual - wine_club_prior as wine_club_variance,
    case 
        when wine_club_prior > 0 
        then ((wine_club_actual - wine_club_prior) / wine_club_prior) * 100 
        else null 
    end as wine_club_variance_pct,
    
    ecomm_actual,
    ecomm_prior,
    ecomm_actual - ecomm_prior as ecomm_variance,
    case 
        when ecomm_prior > 0 
        then ((ecomm_actual - ecomm_prior) / ecomm_prior) * 100 
        else null 
    end as ecomm_variance_pct,
    
    phone_actual,
    phone_prior,
    phone_actual - phone_prior as phone_variance,
    case 
        when phone_prior > 0 
        then ((phone_actual - phone_prior) / phone_prior) * 100 
        else null 
    end as phone_variance_pct,
    
    event_fees_actual,
    event_fees_prior,
    event_fees_actual - event_fees_prior as event_fees_variance,
    case 
        when event_fees_prior > 0 
        then ((event_fees_actual - event_fees_prior) / event_fees_prior) * 100 
        else null 
    end as event_fees_variance_pct,
    
    event_wine_actual,
    event_wine_prior,
    event_wine_actual - event_wine_prior as event_wine_variance,
    case 
        when event_wine_prior > 0 
        then ((event_wine_actual - event_wine_prior) / event_wine_prior) * 100 
        else null 
    end as event_wine_variance_pct,
    
    shipping_actual,
    shipping_prior,
    shipping_actual - shipping_prior as shipping_variance,
    case 
        when shipping_prior > 0 
        then ((shipping_actual - shipping_prior) / shipping_prior) * 100 
        else null 
    end as shipping_variance_pct,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from tasting_room_metrics 