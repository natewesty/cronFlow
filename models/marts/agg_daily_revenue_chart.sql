{{
  config(
    materialized='table'
  )
}}

with date_range as (
    -- Get the date range: from start of previous fiscal year to current date
    select 
        (select current_date_pacific from {{ ref('dim_date') }} limit 1) as current_date,
        (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '2 years' as start_date
),

daily_tasting_room_wine as (
    select
        fo.order_date_key as date_day,
        sum(fo.subtotal) as tasting_room_wine_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.channel = 'POS'
    and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
    and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
    and fo.event_fee_or_wine is null
    and fo.event_specific_sale is null
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_tasting_room_fees as (
    select
        date(ftr.reservation_datetime) as date_day,
        sum(ftr.final_total) as tasting_room_fees_revenue
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Tasting Room'
    and date(ftr.reservation_datetime) >= dr.start_date
    and date(ftr.reservation_datetime) <= dr.current_date
    group by date(ftr.reservation_datetime)
),

daily_wine_club_orders as (
    select
        fo.order_date_key as date_day,
        sum(fo.subtotal) as wine_club_orders_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.channel = 'Club'
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_wine_club_fees as (
    select
        date(ftr.reservation_datetime) as date_day,
        sum(ftr.final_total) as wine_club_fees_revenue
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Club'
    and date(ftr.reservation_datetime) >= dr.start_date
    and date(ftr.reservation_datetime) <= dr.current_date
    group by date(ftr.reservation_datetime)
),

daily_ecomm as (
    select
        fo.order_date_key as date_day,
        sum(fo.subtotal) as ecomm_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.channel = 'Web'
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_phone as (
    select
        fo.order_date_key as date_day,
        sum(fo.subtotal) as phone_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.channel = 'Inbound'
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_event_fees_orders as (
    select
        fo.order_date_key as date_day,
        sum(fo.subtotal) as event_fees_orders_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.event_fee_or_wine = 'Event Fee'
    and fo.event_specific_sale = 'true'
    and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_event_fees_reservations as (
    select
        date(ftr.reservation_datetime) as date_day,
        sum(ftr.final_total) as event_fees_reservations_revenue
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Event'
    and date(ftr.reservation_datetime) >= dr.start_date
    and date(ftr.reservation_datetime) <= dr.current_date
    group by date(ftr.reservation_datetime)
),

daily_event_wine as (
    select
        fo.order_date_key as date_day,
        sum(fo.subtotal) as event_wine_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.event_fee_or_wine = 'Event Wine'
    and fo.event_specific_sale = 'true'
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_shipping_revenue as (
    select
        fo.order_date_key as date_day,
        sum(fo.shipping) as shipping_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

-- Create a complete date spine for the two-year period
date_spine as (
    select 
        dd.date_day,
        dd.fiscal_year,
        dd.fiscal_year_name,
        dd.fiscal_month,
        dd.fiscal_quarter,
        dd.month_name,
        dd.weekday_name,
        case 
            when dd.fiscal_year = (select fiscal_year from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1))
            then 'Current'
            when dd.fiscal_year = (select fiscal_year from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) - 1
            then 'Previous'
            else 'Other'
        end as fiscal_year_period
    from {{ ref('dim_date') }} dd
    cross join date_range dr
    where dd.date_day >= dr.start_date
    and dd.date_day <= dr.current_date
)

select
    ds.date_day,
    ds.fiscal_year,
    ds.fiscal_year_name,
    ds.fiscal_month,
    ds.fiscal_quarter,
    ds.month_name,
    ds.weekday_name,
    ds.fiscal_year_period,
    
    -- Tasting Room Revenue
    coalesce(dtrw.tasting_room_wine_revenue, 0) as tasting_room_wine_revenue,
    coalesce(dtrf.tasting_room_fees_revenue, 0) as tasting_room_fees_revenue,
    coalesce(dtrw.tasting_room_wine_revenue, 0) + coalesce(dtrf.tasting_room_fees_revenue, 0) as tasting_room_total_revenue,
    
    -- Wine Club Revenue
    coalesce(dwco.wine_club_orders_revenue, 0) as wine_club_orders_revenue,
    coalesce(dwcf.wine_club_fees_revenue, 0) as wine_club_fees_revenue,
    coalesce(dwco.wine_club_orders_revenue, 0) + coalesce(dwcf.wine_club_fees_revenue, 0) as wine_club_total_revenue,
    
    -- eComm Revenue
    coalesce(de.ecomm_revenue, 0) as ecomm_revenue,
    
    -- Phone Revenue
    coalesce(dp.phone_revenue, 0) as phone_revenue,
    
    -- Event Fees Revenue
    coalesce(defo.event_fees_orders_revenue, 0) as event_fees_orders_revenue,
    coalesce(defr.event_fees_reservations_revenue, 0) as event_fees_reservations_revenue,
    coalesce(defo.event_fees_orders_revenue, 0) + coalesce(defr.event_fees_reservations_revenue, 0) as event_fees_total_revenue,
    
    -- Event Wine Revenue
    coalesce(dew.event_wine_revenue, 0) as event_wine_revenue,
    
    -- Shipping Revenue
    coalesce(dsr.shipping_revenue, 0) as shipping_revenue,
    
    -- Total Daily Revenue
    coalesce(dtrw.tasting_room_wine_revenue, 0) + 
    coalesce(dtrf.tasting_room_fees_revenue, 0) + 
    coalesce(dwco.wine_club_orders_revenue, 0) + 
    coalesce(dwcf.wine_club_fees_revenue, 0) + 
    coalesce(de.ecomm_revenue, 0) + 
    coalesce(dp.phone_revenue, 0) + 
    coalesce(defo.event_fees_orders_revenue, 0) + 
    coalesce(defr.event_fees_reservations_revenue, 0) + 
    coalesce(dew.event_wine_revenue, 0) + 
    coalesce(dsr.shipping_revenue, 0) as total_daily_revenue

from date_spine ds
left join daily_tasting_room_wine dtrw on ds.date_day = dtrw.date_day
left join daily_tasting_room_fees dtrf on ds.date_day = dtrf.date_day
left join daily_wine_club_orders dwco on ds.date_day = dwco.date_day
left join daily_wine_club_fees dwcf on ds.date_day = dwcf.date_day
left join daily_ecomm de on ds.date_day = de.date_day
left join daily_phone dp on ds.date_day = dp.date_day
left join daily_event_fees_orders defo on ds.date_day = defo.date_day
left join daily_event_fees_reservations defr on ds.date_day = defr.date_day
left join daily_event_wine dew on ds.date_day = dew.date_day
left join daily_shipping_revenue dsr on ds.date_day = dsr.date_day
order by ds.date_day
