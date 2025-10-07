{{
  config(
    materialized='table'
  )
}}

with date_range as (
    -- Get date range from beginning of previous fiscal year to current date
    select 
        (select current_date_pacific from {{ ref('dim_date') }} limit 1) as current_date,
        date('2023-07-01') as start_date  -- Beginning of FY2024 (using END year naming)
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
    and fo.event_fee_or_wine is null or fo.event_fee_or_wine = 'false'
    and fo.event_specific_sale is null
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

daily_tasting_room_fees as (
    select
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        sum(ftr.final_total) as tasting_room_fees_revenue
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Tasting Room'
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
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
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        sum(ftr.final_total) as wine_club_fees_revenue
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Club'
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
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
        coalesce(date(fo.event_revenue_realization_date), fo.order_date_key) as date_day,
        sum(fo.subtotal) as event_fees_orders_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.event_fee_or_wine = 'Event Fee'
    and fo.event_specific_sale = 'true'
    and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
    and coalesce(date(fo.event_revenue_realization_date), fo.order_date_key) >= dr.start_date
    and coalesce(date(fo.event_revenue_realization_date), fo.order_date_key) <= dr.current_date
    group by coalesce(date(fo.event_revenue_realization_date), fo.order_date_key)
),

daily_event_fees_reservations as (
    select
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        sum(ftr.final_total) as event_fees_reservations_revenue
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Event'
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
),

daily_event_wine as (
    select
        coalesce(date(fo.event_revenue_realization_date), fo.order_date_key) as date_day,
        sum(fo.subtotal) as event_wine_revenue
    from {{ ref('fct_order') }} fo
    left join {{ ref('dim_date') }} dd on fo.order_date_key = dd.date_day
    cross join date_range dr
    where fo.event_fee_or_wine = 'Event Wine'
    and fo.event_specific_sale = 'true'
    and coalesce(date(fo.event_revenue_realization_date), fo.order_date_key) >= dr.start_date
    and coalesce(date(fo.event_revenue_realization_date), fo.order_date_key) <= dr.current_date
    group by coalesce(date(fo.event_revenue_realization_date), fo.order_date_key)
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

-- Traffic metrics
daily_reservations as (
    select
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        count(*) as total_reservations,
        sum(ftr.party_size) as total_visitors,
        avg(ftr.party_size) as avg_party_size
    from {{ ref('fct_tock_reservation') }} ftr
    cross join date_range dr
    where to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
),

-- Tasting Room Guests
daily_tasting_room_guests as (
    select
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        sum(ftr.party_size) as tasting_room_guests
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Tasting Room'
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
),

-- Event Guests
daily_event_guests as (
    select
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        sum(ftr.party_size) as event_guests
    from {{ ref('fct_tock_reservation') }} ftr
    left join {{ ref('dim_experience') }} de on ftr.experience_name = de.experience
    cross join date_range dr
    where de.attribution = 'Event'
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
),

-- Average Tasting Fee Per Guest
daily_tasting_fee_per_guest as (
    select
        to_date(ftr.reservation_datetime, 'MM-DD-YYYY') as date_day,
        case 
            when sum(ftr.party_size) > 0 
            then sum(ftr.final_total) / sum(ftr.party_size)
            else 0 
        end as avg_tasting_fee_per_guest
    from {{ ref('fct_tock_reservation') }} ftr
    cross join date_range dr
    where to_date(ftr.reservation_datetime, 'MM-DD-YYYY') >= dr.start_date
    and to_date(ftr.reservation_datetime, 'MM-DD-YYYY') <= dr.current_date
    group by to_date(ftr.reservation_datetime, 'MM-DD-YYYY')
),

-- Tasting Room Orders count
daily_tasting_room_orders as (
    select
        fo.order_date_key as date_day,
        count(*) as tasting_room_order_count
    from {{ ref('fct_order') }} fo
    cross join date_range dr
    where fo.channel = 'POS'
    and (fo.external_order_vendor is null or fo.external_order_vendor <> 'Tock')
    and (fo.tasting_lounge is null or fo.tasting_lounge = 'false')
    and fo.event_fee_or_wine is null or fo.event_fee_or_wine = 'false'
    and fo.event_specific_sale is null
    and fo.order_date_key >= dr.start_date
    and fo.order_date_key <= dr.current_date
    group by fo.order_date_key
),

-- Total 9L Sold
daily_9l_sold as (
    select
        date(foi.paid_at) as date_day,
        sum(foi.quantity) / 12.0 as total_9l_sold
    from {{ ref('fct_order_item') }} foi
    cross join date_range dr
    where foi.item_type = 'Wine'
    and date(foi.paid_at) >= dr.start_date
    and date(foi.paid_at) <= dr.current_date
    group by date(foi.paid_at)
),

-- Club membership metrics
daily_club_signups as (
    select
        date(dcm.signup_at) as date_day,
        count(*) as new_member_acquisition
    from {{ ref('dim_club_membership') }} dcm
    cross join date_range dr
    where dcm.status = 'Active'
    and date(dcm.signup_at) >= dr.start_date
    and date(dcm.signup_at) <= dr.current_date
    group by date(dcm.signup_at)
),

daily_club_cancellations as (
    select
        date(dcm.cancel_at) as date_day,
        count(*) as existing_member_attrition
    from {{ ref('dim_club_membership') }} dcm
    cross join date_range dr
    where date(dcm.cancel_at) >= dr.start_date
    and date(dcm.cancel_at) <= dr.current_date
    group by date(dcm.cancel_at)
),

-- Create a date spine for all dates in range
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
),

-- Total Active Club Membership (count as of each date)
active_club_members_by_date as (
    select
        ds.date_day,
        count(*) as total_active_club_membership
    from date_spine ds
    cross join {{ ref('dim_club_membership') }} dcm
    where dcm.status = 'Active'
    and date(dcm.signup_at) <= ds.date_day
    and (dcm.cancel_at is null or date(dcm.cancel_at) > ds.date_day)
    group by ds.date_day
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
    coalesce(dsr.shipping_revenue, 0) as total_daily_revenue,
    
    -- Traffic Metrics
    coalesce(dr.total_reservations, 0) as total_reservations,
    coalesce(dr.total_visitors, 0) as total_visitors,
    coalesce(dr.avg_party_size, 0) as avg_party_size,
    
    -- Guest Metrics
    coalesce(dtrg.tasting_room_guests, 0) as tasting_room_guests,
    coalesce(deg.event_guests, 0) as event_guests,
    coalesce(dtfpg.avg_tasting_fee_per_guest, 0) as avg_tasting_fee_per_guest,
    
    -- Tasting Room Orders Per Guest (as percentage)
    case 
        when coalesce(dtrg.tasting_room_guests, 0) > 0 
        then (coalesce(dtro.tasting_room_order_count, 0)::numeric / dtrg.tasting_room_guests) * 100
        else 0 
    end as tasting_room_orders_per_guest_pct,
    
    -- Wine Sales Metrics
    coalesce(d9l.total_9l_sold, 0) as total_9l_sold,
    
    -- Club Membership Metrics
    coalesce(acm.total_active_club_membership, 0) as total_active_club_membership,
    coalesce(dcs.new_member_acquisition, 0) as new_member_acquisition,
    coalesce(dcc.existing_member_attrition, 0) as existing_member_attrition,
    coalesce(dcs.new_member_acquisition, 0) - coalesce(dcc.existing_member_attrition, 0) as club_population_net_gain_loss,
    
    -- Club Conversion Per Taster (as percentage)
    case 
        when coalesce(dcs.new_member_acquisition, 0) > 0 
        then (coalesce(dtrg.tasting_room_guests, 0)::numeric / dcs.new_member_acquisition) * 100
        else 0 
    end as club_conversion_per_taster_pct

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
left join daily_reservations dr on ds.date_day = dr.date_day
left join daily_tasting_room_guests dtrg on ds.date_day = dtrg.date_day
left join daily_event_guests deg on ds.date_day = deg.date_day
left join daily_tasting_fee_per_guest dtfpg on ds.date_day = dtfpg.date_day
left join daily_tasting_room_orders dtro on ds.date_day = dtro.date_day
left join daily_9l_sold d9l on ds.date_day = d9l.date_day
left join daily_club_signups dcs on ds.date_day = dcs.date_day
left join daily_club_cancellations dcc on ds.date_day = dcc.date_day
left join active_club_members_by_date acm on ds.date_day = acm.date_day
order by ds.date_day

