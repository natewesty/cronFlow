{{
  config(
    materialized='table'
  )
}}

with current_periods as (
    select 
        current_date_pacific,
        fiscal_year as current_fiscal_year
    from {{ ref('dim_date') }} 
    where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
),

quarterly_metrics as (
    select
        -- Tasting Room Wine Q1: Current fiscal year Q1
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
            and dd.fiscal_quarter = 1
        ), 0) as tasting_room_wine_q1,
        
        -- Tasting Room Wine Q1 Prior: Previous fiscal year Q1
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
            and dd.fiscal_quarter = 1
        ), 0) as tasting_room_wine_q1_prior,
        
        -- Tasting Room Wine Q2: Current fiscal year Q2
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
            and dd.fiscal_quarter = 2
        ), 0) as tasting_room_wine_q2,
        
        -- Tasting Room Wine Q2 Prior: Previous fiscal year Q2
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
            and dd.fiscal_quarter = 2
        ), 0) as tasting_room_wine_q2_prior,
        
        -- Tasting Room Wine Q3: Current fiscal year Q3
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
            and dd.fiscal_quarter = 3
        ), 0) as tasting_room_wine_q3,
        
        -- Tasting Room Wine Q3 Prior: Previous fiscal year Q3
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
            and dd.fiscal_quarter = 3
        ), 0) as tasting_room_wine_q3_prior,
        
        -- Tasting Room Wine Q4: Current fiscal year Q4
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
            and dd.fiscal_quarter = 4
        ), 0) as tasting_room_wine_q4,
        
        -- Tasting Room Wine Q4 Prior: Previous fiscal year Q4
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
            and dd.fiscal_quarter = 4
        ), 0) as tasting_room_wine_q4_prior,
        
        -- Tasting Room Fees Q1: Current fiscal year Q1
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 1
        ), 0) as tasting_room_fees_q1,
        
        -- Tasting Room Fees Q1 Prior: Previous fiscal year Q1
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 1
        ), 0) as tasting_room_fees_q1_prior,
        
        -- Tasting Room Fees Q2: Current fiscal year Q2
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 2
        ), 0) as tasting_room_fees_q2,
        
        -- Tasting Room Fees Q2 Prior: Previous fiscal year Q2
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 2
        ), 0) as tasting_room_fees_q2_prior,
        
        -- Tasting Room Fees Q3: Current fiscal year Q3
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 3
        ), 0) as tasting_room_fees_q3,
        
        -- Tasting Room Fees Q3 Prior: Previous fiscal year Q3
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 3
        ), 0) as tasting_room_fees_q3_prior,
        
        -- Tasting Room Fees Q4: Current fiscal year Q4
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 4
        ), 0) as tasting_room_fees_q4,
        
        -- Tasting Room Fees Q4 Prior: Previous fiscal year Q4
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 4
        ), 0) as tasting_room_fees_q4_prior,
        
        -- Wine Club Q1: Current fiscal year Q1
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 1
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 1
        ), 0) as wine_club_q1,
        
        -- Wine Club Q1 Prior: Previous fiscal year Q1
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 1
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 1
        ), 0) as wine_club_q1_prior,
        
        -- Wine Club Q2: Current fiscal year Q2
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 2
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 2
        ), 0) as wine_club_q2,
        
        -- Wine Club Q2 Prior: Previous fiscal year Q2
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 2
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 2
        ), 0) as wine_club_q2_prior,
        
        -- Wine Club Q3: Current fiscal year Q3
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 3
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 3
        ), 0) as wine_club_q3,
        
        -- Wine Club Q3 Prior: Previous fiscal year Q3
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 3
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 3
        ), 0) as wine_club_q3_prior,
        
        -- Wine Club Q4: Current fiscal year Q4
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 4
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = 4
        ), 0) as wine_club_q4,
        
        -- Wine Club Q4 Prior: Previous fiscal year Q4
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 4
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on date(ftr.reservation_datetime) = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year - 1
            and dd.fiscal_quarter = 4
        ), 0) as wine_club_q4_prior
)

select
    (select current_date_pacific from {{ ref('dim_date') }} limit 1) as report_date,
    
    -- Tasting Room Wine Quarterly Metrics
    tasting_room_wine_q1,
    tasting_room_wine_q1_prior,
    tasting_room_wine_q1 - tasting_room_wine_q1_prior as tasting_room_wine_q1_variance,
    case 
        when tasting_room_wine_q1_prior > 0 
        then ((tasting_room_wine_q1 - tasting_room_wine_q1_prior) / tasting_room_wine_q1_prior) * 100 
        else null 
    end as tasting_room_wine_q1_variance_pct,
    
    tasting_room_wine_q2,
    tasting_room_wine_q2_prior,
    tasting_room_wine_q2 - tasting_room_wine_q2_prior as tasting_room_wine_q2_variance,
    case 
        when tasting_room_wine_q2_prior > 0 
        then ((tasting_room_wine_q2 - tasting_room_wine_q2_prior) / tasting_room_wine_q2_prior) * 100 
        else null 
    end as tasting_room_wine_q2_variance_pct,
    
    tasting_room_wine_q3,
    tasting_room_wine_q3_prior,
    tasting_room_wine_q3 - tasting_room_wine_q3_prior as tasting_room_wine_q3_variance,
    case 
        when tasting_room_wine_q3_prior > 0 
        then ((tasting_room_wine_q3 - tasting_room_wine_q3_prior) / tasting_room_wine_q3_prior) * 100 
        else null 
    end as tasting_room_wine_q3_variance_pct,
    
    tasting_room_wine_q4,
    tasting_room_wine_q4_prior,
    tasting_room_wine_q4 - tasting_room_wine_q4_prior as tasting_room_wine_q4_variance,
    case 
        when tasting_room_wine_q4_prior > 0 
        then ((tasting_room_wine_q4 - tasting_room_wine_q4_prior) / tasting_room_wine_q4_prior) * 100 
        else null 
    end as tasting_room_wine_q4_variance_pct,
    
    -- Tasting Room Fees Quarterly Metrics
    tasting_room_fees_q1,
    tasting_room_fees_q1_prior,
    tasting_room_fees_q1 - tasting_room_fees_q1_prior as tasting_room_fees_q1_variance,
    case 
        when tasting_room_fees_q1_prior > 0 
        then ((tasting_room_fees_q1 - tasting_room_fees_q1_prior) / tasting_room_fees_q1_prior) * 100 
        else null 
    end as tasting_room_fees_q1_variance_pct,
    
    tasting_room_fees_q2,
    tasting_room_fees_q2_prior,
    tasting_room_fees_q2 - tasting_room_fees_q2_prior as tasting_room_fees_q2_variance,
    case 
        when tasting_room_fees_q2_prior > 0 
        then ((tasting_room_fees_q2 - tasting_room_fees_q2_prior) / tasting_room_fees_q2_prior) * 100 
        else null 
    end as tasting_room_fees_q2_variance_pct,
    
    tasting_room_fees_q3,
    tasting_room_fees_q3_prior,
    tasting_room_fees_q3 - tasting_room_fees_q3_prior as tasting_room_fees_q3_variance,
    case 
        when tasting_room_fees_q3_prior > 0 
        then ((tasting_room_fees_q3 - tasting_room_fees_q3_prior) / tasting_room_fees_q3_prior) * 100 
        else null 
    end as tasting_room_fees_q3_variance_pct,
    
    tasting_room_fees_q4,
    tasting_room_fees_q4_prior,
    tasting_room_fees_q4 - tasting_room_fees_q4_prior as tasting_room_fees_q4_variance,
    case 
        when tasting_room_fees_q4_prior > 0 
        then ((tasting_room_fees_q4 - tasting_room_fees_q4_prior) / tasting_room_fees_q4_prior) * 100 
        else null 
    end as tasting_room_fees_q4_variance_pct,
    
    -- Wine Club Quarterly Metrics
    wine_club_q1,
    wine_club_q1_prior,
    wine_club_q1 - wine_club_q1_prior as wine_club_q1_variance,
    case 
        when wine_club_q1_prior > 0 
        then ((wine_club_q1 - wine_club_q1_prior) / wine_club_q1_prior) * 100 
        else null 
    end as wine_club_q1_variance_pct,
    
    wine_club_q2,
    wine_club_q2_prior,
    wine_club_q2 - wine_club_q2_prior as wine_club_q2_variance,
    case 
        when wine_club_q2_prior > 0 
        then ((wine_club_q2 - wine_club_q2_prior) / wine_club_q2_prior) * 100 
        else null 
    end as wine_club_q2_variance_pct,
    
    wine_club_q3,
    wine_club_q3_prior,
    wine_club_q3 - wine_club_q3_prior as wine_club_q3_variance,
    case 
        when wine_club_q3_prior > 0 
        then ((wine_club_q3 - wine_club_q3_prior) / wine_club_q3_prior) * 100 
        else null 
    end as wine_club_q3_variance_pct,
    
    wine_club_q4,
    wine_club_q4_prior,
    wine_club_q4 - wine_club_q4_prior as wine_club_q4_variance,
    case 
        when wine_club_q4_prior > 0 
        then ((wine_club_q4 - wine_club_q4_prior) / wine_club_q4_prior) * 100 
        else null 
    end as wine_club_q4_variance_pct,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from quarterly_metrics
