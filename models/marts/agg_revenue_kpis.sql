{{
  config(
    materialized='table'
  )
}}

with current_periods as (
    select 
        current_date_pacific,
        fiscal_year as current_fiscal_year,
        fiscal_month as current_fiscal_month,
        fiscal_quarter as current_fiscal_quarter
    from {{ ref('dim_date') }} 
    where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
),

tasting_room_metrics as (
    select
        -- Tasting Room Wine Actual: Current fiscal year
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
        ), 0) as tasting_room_wine_actual,
        
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
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) as tasting_room_fees_actual,
        
        -- Tasting Room Fees Month-to-Date: Current fiscal month
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= cp.current_date_pacific
        ), 0) as tasting_room_fees_month_to_date,
        
        -- Tasting Room Fees Current Month Total: Entire current fiscal month
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as tasting_room_fees_current_month,
        
        -- Tasting Room Fees Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as tasting_room_fees_current_quarter,
        
        -- Tasting Room Fees Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            where de.attribution = 'Tasting Room'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as tasting_room_fees_prior,
        
        -- Wine Club Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) as wine_club_actual,
        
        -- Wine Club Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= cp.current_date_pacific
        ), 0) as wine_club_month_to_date,
        
        -- Wine Club Current Month Total: Entire current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as wine_club_current_month,
        
        -- Wine Club Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Club'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as wine_club_current_quarter,
        
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
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            where de.attribution = 'Club'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as wine_club_prior,
        
        -- eComm Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Web'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) as ecomm_actual,
        
        -- eComm Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Web'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) as ecomm_month_to_date,
        
        -- eComm Current Month Total: Entire current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Web'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as ecomm_current_month,
        
        -- eComm Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Web'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as ecomm_current_quarter,
        
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
            cross join current_periods cp
            where fo.channel = 'Inbound'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) as phone_actual,
        
        -- Phone Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Inbound'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) as phone_month_to_date,
        
        -- Phone Current Month Total: Entire current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Inbound'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as phone_current_month,
        
        -- Phone Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.channel = 'Inbound'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as phone_current_quarter,
        
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
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = 'true'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Event'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) as event_fees_actual,
        
        -- Event Fees Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = 'true'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Event'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= cp.current_date_pacific
        ), 0) as event_fees_month_to_date,
        
        -- Event Fees Current Month Total: Entire current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = 'true'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Event'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as event_fees_current_month,
        
        -- Event Fees Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = 'true'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            cross join current_periods cp
            where de.attribution = 'Event'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as event_fees_current_quarter,
        
        -- Event Fees Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.event_fee_or_wine = 'Event Fee'
            and fo.event_specific_sale = 'true'
            and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
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
        ), 0) + coalesce((
            select sum(ftr.final_total)
            from {{ ref('fct_tock_reservation') }} ftr
            left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
            left join {{ ref('dim_date') }} dd on to_date(ftr.reservation_datetime, 'MM-DD-YYYY') = dd.date_day
            where de.attribution = 'Event'
            and dd.fiscal_year = (
                select fiscal_year 
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)
            ) - 1
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= (
                select date_trunc('year', date_day)::date + interval '6 months'
                from {{ ref('dim_date') }} 
                where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
            )::date
            and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year'
        ), 0) as event_fees_prior,
        
        -- Event Wine Actual: Current fiscal year
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = 'true'
            and dd.fiscal_year = cp.current_fiscal_year
        ), 0) as event_wine_actual,
        
        -- Event Wine Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = 'true'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) as event_wine_month_to_date,
        
        -- Event Wine Current Month Total: Entire current fiscal month
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = 'true'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as event_wine_current_month,
        
        -- Event Wine Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = 'true'
            and dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as event_wine_current_quarter,
        
        -- Event Wine Prior: Previous fiscal year (same date range)
        coalesce((
            select sum(fo.subtotal)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            where fo.event_fee_or_wine = 'Event Wine'
            and fo.event_specific_sale = 'true'
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
            cross join current_periods cp
            where dd.fiscal_year = cp.current_fiscal_year
        ), 0) as shipping_actual,
        
        -- Shipping Month-to-Date: Current fiscal month
        coalesce((
            select sum(fo.shipping)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
            and fo.order_date_key <= cp.current_date_pacific
        ), 0) as shipping_month_to_date,
        
        -- Shipping Current Month Total: Entire current fiscal month
        coalesce((
            select sum(fo.shipping)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_month = cp.current_fiscal_month
        ), 0) as shipping_current_month,
        
        -- Shipping Current Quarter Total: Entire current fiscal quarter
        coalesce((
            select sum(fo.shipping)
            from {{ ref('fct_order') }} fo
            left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
            cross join current_periods cp
            where dd.fiscal_year = cp.current_fiscal_year
            and dd.fiscal_quarter = cp.current_fiscal_quarter
        ), 0) as shipping_current_quarter,
        
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
    
    -- Tasting Room Wine Metrics
    tasting_room_wine_actual,
    tasting_room_wine_prior,
    tasting_room_wine_actual - tasting_room_wine_prior as tasting_room_wine_variance,
    case 
        when tasting_room_wine_prior > 0 
        then ((tasting_room_wine_actual - tasting_room_wine_prior) / tasting_room_wine_prior) * 100 
        else null 
    end as tasting_room_wine_variance_pct,
    tasting_room_wine_month_to_date,
    tasting_room_wine_current_month,
    tasting_room_wine_current_quarter,
    
    -- Tasting Room Fees Metrics
    tasting_room_fees_actual,
    tasting_room_fees_prior,
    tasting_room_fees_actual - tasting_room_fees_prior as tasting_room_fees_variance,
    case 
        when tasting_room_fees_prior > 0 
        then ((tasting_room_fees_actual - tasting_room_fees_prior) / tasting_room_fees_prior) * 100 
        else null 
    end as tasting_room_fees_variance_pct,
    tasting_room_fees_month_to_date,
    tasting_room_fees_current_month,
    tasting_room_fees_current_quarter,
    
    -- Wine Club Metrics
    wine_club_actual,
    wine_club_prior,
    wine_club_actual - wine_club_prior as wine_club_variance,
    case 
        when wine_club_prior > 0 
        then ((wine_club_actual - wine_club_prior) / wine_club_prior) * 100 
        else null 
    end as wine_club_variance_pct,
    wine_club_month_to_date,
    wine_club_current_month,
    wine_club_current_quarter,
    
    -- eComm Metrics
    ecomm_actual,
    ecomm_prior,
    ecomm_actual - ecomm_prior as ecomm_variance,
    case 
        when ecomm_prior > 0 
        then ((ecomm_actual - ecomm_prior) / ecomm_prior) * 100 
        else null 
    end as ecomm_variance_pct,
    ecomm_month_to_date,
    ecomm_current_month,
    ecomm_current_quarter,
    
    -- Phone Metrics
    phone_actual,
    phone_prior,
    phone_actual - phone_prior as phone_variance,
    case 
        when phone_prior > 0 
        then ((phone_actual - phone_prior) / phone_prior) * 100 
        else null 
    end as phone_variance_pct,
    phone_month_to_date,
    phone_current_month,
    phone_current_quarter,
    
    -- Event Fees Metrics
    event_fees_actual,
    event_fees_prior,
    event_fees_actual - event_fees_prior as event_fees_variance,
    case 
        when event_fees_prior > 0 
        then ((event_fees_actual - event_fees_prior) / event_fees_prior) * 100 
        else null 
    end as event_fees_variance_pct,
    event_fees_month_to_date,
    event_fees_current_month,
    event_fees_current_quarter,
    
    -- Event Wine Metrics
    event_wine_actual,
    event_wine_prior,
    event_wine_actual - event_wine_prior as event_wine_variance,
    case 
        when event_wine_prior > 0 
        then ((event_wine_actual - event_wine_prior) / event_wine_prior) * 100 
        else null 
    end as event_wine_variance_pct,
    event_wine_month_to_date,
    event_wine_current_month,
    event_wine_current_quarter,
    
    -- Shipping Metrics
    shipping_actual,
    shipping_prior,
    shipping_actual - shipping_prior as shipping_variance,
    case 
        when shipping_prior > 0 
        then ((shipping_actual - shipping_prior) / shipping_prior) * 100 
        else null 
    end as shipping_variance_pct,
    shipping_month_to_date,
    shipping_current_month,
    shipping_current_quarter,
    
    -- Current fiscal year info (Pacific Time)
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) as current_fiscal_year,
    (select fiscal_year_name from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '1 year') as previous_fiscal_year

from tasting_room_metrics 