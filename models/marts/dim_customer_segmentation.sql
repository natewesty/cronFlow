{{ config(materialized='table') }}

with customer_base as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        c.state_code,
        c.postal_code,
        c.country_code,
        c.primary_email,
        c.email_mkt_status,
        c.has_account,
        c.order_count,
        c.lifetime_value_dollars,
        c.lifetime_gross_profit_dollars,
        c.is_active_club_member,
        c.acquisition_channel,
        c.customer_tags,
        c.created_at,
        c.updated_at
    from {{ ref('dim_customer') }} c
    where c.postal_code is not null 
      and length(trim(c.postal_code)) >= 5
),

ruca_mapping as (
    select 
        "ZIPCode"::text as postal_code,
        "State" as state,
        "POName" as place_name,
        "PrimaryRUCA" as primary_ruca_code,
        "SecondaryRUCA" as secondary_ruca_code,
        -- RUCA classification categories
        case 
            when "PrimaryRUCA" in (1, 2, 3) then 'Metropolitan'
            when "PrimaryRUCA" in (4, 5, 6) then 'Micropolitan'
            when "PrimaryRUCA" in (7, 8, 9) then 'Small Town'
            when "PrimaryRUCA" = 10 then 'Rural'
            else 'Unknown'
        end as ruca_category,
        -- Detailed RUCA descriptions
        case 
            when "PrimaryRUCA" = 1 then 'Metropolitan area core: primary flow within an urbanized area (UA)'
            when "PrimaryRUCA" = 2 then 'Metropolitan area high commuting: primary flow 30% or more to a UA'
            when "PrimaryRUCA" = 3 then 'Metropolitan area low commuting: primary flow 10% to 30% to a UA'
            when "PrimaryRUCA" = 4 then 'Micropolitan area core: primary flow within an urban cluster of 10,000 to 49,999 (large UC)'
            when "PrimaryRUCA" = 5 then 'Micropolitan high commuting: primary flow 30% or more to a large UC'
            when "PrimaryRUCA" = 6 then 'Micropolitan low commuting: primary flow 10% to 30% to a large UC'
            when "PrimaryRUCA" = 7 then 'Small town core: primary flow within an urban cluster of 2,500 to 9,999 (small UC)'
            when "PrimaryRUCA" = 8 then 'Small town high commuting: primary flow 30% or more to a small UC'
            when "PrimaryRUCA" = 9 then 'Small town low commuting: primary flow 10% to 30% to a small UC'
            when "PrimaryRUCA" = 10 then 'Rural areas: primary flow to a tract outside a UA or UC'
            else 'Unknown'
        end as ruca_description
    from {{ ref('dim_ruca') }}
),

-- Order-level metrics from order items
order_item_metrics as (
    select
        oi.customer_id,
        max(oi.paid_date) as last_order_date,
        min(oi.paid_date) as first_order_date,
        (current_date - max(oi.paid_date)) as recency_days,
        count(distinct case when oi.paid_date >= (current_date - interval '1 year') and oi.item_type = 'Wine' then oi.order_id end) as orders_12mo,
        sum(case when oi.paid_date >= (current_date - interval '1 year') and oi.item_type = 'Wine' then oi.product_subtotal end) as revenue_12mo,
        nullif(sum(case when oi.paid_date >= (current_date - interval '1 year') and oi.item_type = 'Wine' then oi.product_subtotal end), 0)
            / nullif(count(distinct case when oi.paid_date >= (current_date - interval '1 year') and oi.item_type = 'Wine' then oi.order_id end), 0) as aov_12mo,
        avg(case when oi.item_type = 'Wine' then oi.item_price end) as avg_unit_price_all_time,
        count(distinct case when oi.item_type = 'Wine' then oi.order_id end) as total_orders,
        sum(case when oi.item_type = 'Wine' then oi.product_subtotal end) as lifetime_revenue,
        sum(case when oi.item_type = 'Wine' then oi.quantity end) as total_items_purchased,
        -- Calculate average items per order: total items / distinct orders
        nullif(sum(case when oi.item_type = 'Wine' then oi.quantity end), 0)::numeric
            / nullif(count(distinct case when oi.item_type = 'Wine' then oi.order_id end), 0)::numeric as avg_items_per_order
    from {{ ref('fct_order_item') }} oi
    where oi.customer_id is not null
    group by 1
),

-- Order-level metrics from fct_order (channel, delivery, totals)
order_level_metrics as (
    select
        o.customer_id,
        count(distinct o.order_id) as total_order_count,
        count(distinct case when o.order_date_key >= (current_date - interval '1 year') then o.order_id end) as orders_12mo_from_orders,
        sum(case when o.order_date_key >= (current_date - interval '1 year') then o.order_total end) as revenue_12mo_from_orders,
        avg(case when o.order_date_key >= (current_date - interval '1 year') then o.order_total end) as aov_12mo_from_orders,
        -- Channel distribution
        count(distinct case when o.channel = 'Web' then o.order_id end) as online_orders,
        count(distinct case when o.channel = 'POS' then o.order_id end) as pos_orders,
        count(distinct case when o.channel = 'Inbound' then o.order_id end) as inbound_orders,
        count(distinct case when o.channel = 'Club' then o.order_id end) as club_orders,
        -- Delivery method distribution
        count(distinct case when o.delivery_method = 'Pickup' then o.order_id end) as pickup_orders,
        count(distinct case when o.delivery_method = 'Ship' then o.order_id end) as shipping_orders,
        count(distinct case when o.delivery_method = 'Carry Out' then o.order_id end) as carry_out_orders,
        count(distinct case when o.delivery_method not in ('Pickup', 'Shipping', 'Carry Out') or o.delivery_method is null then o.order_id end) as other_delivery_orders,
        -- Payment and fulfillment status
        count(distinct case when o.payment_status = 'Paid' then o.order_id end) as paid_orders,
        max(o.order_date_key) as last_order_date_from_orders
    from {{ ref('fct_order') }} o
    where o.customer_id is not null
    group by 1
),

-- Sales associate attribution
sales_associate_metrics as (
    select
        osoi.customer_id,
        count(distinct osoi.sales_associate_id) as distinct_sales_associates,
        mode() within group (order by osoi.sales_associate) as primary_sales_associate,
        count(distinct case when osoi.sales_associate_id is not null then osoi.order_id end) as orders_with_associate
    from {{ ref('stg_order_item') }} osoi
    where osoi.customer_id is not null
        and osoi.sales_associate_id is not null
    group by 1
),

-- Club membership details
club_membership_details as (
    select
        cm.customer_id,
        count(distinct cm.membership_id) as total_memberships,
        min(cm.signup_at) as first_membership_signup,
        max(cm.signup_at) as last_membership_signup,
        max(cm.cancel_at) as last_membership_cancellation,
        count(distinct case when cm.status = 'Active' then cm.membership_id end) as active_memberships,
        count(distinct case when cm.cancel_at is not null then cm.membership_id end) as cancelled_memberships,
        max(cm.current_shipments) as max_shipments,
        max(cm.signup_at::date) as most_recent_signup_date,
        (current_date - max(cm.signup_at::date)) as days_since_first_signup
    from {{ ref('dim_club_membership') }} cm
    where cm.customer_id is not null
    group by 1
),

-- Price tier and preference signals (varietal/color)
preference_signals as (
    with line as (
        select
            oi.customer_id,
            oi.order_id,
            coalesce(dp.varietal, 'Unknown') as varietal,
            case 
                when dp.wine_type is null then 'Unknown'
                when lower(dp.wine_type) in ('red','white','rosé','rose','sparkling') then 
                    initcap(replace(lower(dp.wine_type), 'rose', 'rosé'))
                else 'Unknown'
            end as color,
            oi.item_price,
            oi.item_type,
            oi.product_subtotal
        from {{ ref('fct_order_item') }} oi
        left join {{ ref('stg_product') }} dp on oi.product_id = dp.product_id
        where oi.item_type = 'Wine'
    ),
    spend as (
        select
            customer_id,
            sum(product_subtotal) as total_spend,
            sum(case when item_price < 95 then product_subtotal end) as spend_value,
            sum(case when item_price >= 95 and item_price < 125 then product_subtotal end) as spend_mid,
            sum(case when item_price >= 125 and item_price < 175 then product_subtotal end) as spend_premium,
            sum(case when item_price >= 175 then product_subtotal end) as spend_luxury
        from line
        group by 1
    ),
    varietal_rank as (
        select
            customer_id,
            varietal,
            sum(product_subtotal) as varietal_spend,
            row_number() over (partition by customer_id order by sum(product_subtotal) desc) as rn,
            1.0 * sum(product_subtotal)
                / nullif(sum(sum(product_subtotal)) over (partition by customer_id), 0) as varietal_share
        from line
        group by 1, 2
    ),
    color_rank as (
        select
            customer_id,
            color,
            sum(product_subtotal) as color_spend,
            row_number() over (partition by customer_id order by sum(product_subtotal) desc) as rn,
            1.0 * sum(product_subtotal)
                / nullif(sum(sum(product_subtotal)) over (partition by customer_id), 0) as color_share
        from line
        group by 1, 2
    )
    select
        s.customer_id,
        case 
            when greatest(coalesce(spend_luxury, 0), coalesce(spend_premium, 0), coalesce(spend_mid, 0), coalesce(spend_value, 0)) = coalesce(spend_luxury, 0) then 'Luxury'
            when greatest(coalesce(spend_premium, 0), coalesce(spend_mid, 0), coalesce(spend_value, 0)) = coalesce(spend_premium, 0) then 'Premium'
            when greatest(coalesce(spend_mid, 0), coalesce(spend_value, 0)) = coalesce(spend_mid, 0) then 'Mid'
            else 'Value'
        end as price_tier_preference,
        nullif(spend_luxury, 0) / nullif(total_spend, 0) as luxury_share,
        nullif(spend_premium, 0) / nullif(total_spend, 0) as premium_share,
        nullif(spend_mid, 0) / nullif(total_spend, 0) as mid_share,
        nullif(spend_value, 0) / nullif(total_spend, 0) as value_share,
        vr.varietal as top_varietal,
        vr.varietal_share as top_varietal_share,
        cr.color as top_color,
        cr.color_share as top_color_share
    from spend s
    left join varietal_rank vr on s.customer_id = vr.customer_id and vr.rn = 1
    left join color_rank cr on s.customer_id = cr.customer_id and cr.rn = 1
),

-- Seasonality signals: favorite purchase month by spend
seasonality as (
    select
        oi.customer_id,
        date_part('month', oi.paid_date)::int as order_month,
        sum(oi.product_subtotal) as month_spend,
        row_number() over (partition by oi.customer_id order by sum(oi.product_subtotal) desc) as rn
    from {{ ref('fct_order_item') }} oi
    where oi.item_type = 'Wine'
    group by 1, 2
),

-- Seasonal patterns: color-by-month combinations
seasonal_patterns as (
    select
        oi.customer_id,
        date_part('month', oi.paid_date)::int as order_month,
        case 
            when dp.wine_type is null then 'Unknown'
            when lower(dp.wine_type) in ('red','white','rosé','rose','sparkling') then 
                initcap(replace(lower(dp.wine_type), 'rose', 'rosé'))
            else 'Unknown'
        end as color,
        sum(oi.product_subtotal) as month_color_spend
    from {{ ref('fct_order_item') }} oi
    left join {{ ref('stg_product') }} dp on oi.product_id = dp.product_id
    where oi.item_type = 'Wine'
      and oi.paid_date is not null
    group by 1, 2, 3
),

-- Seasonal affinity flags: multiple patterns a customer can match
seasonal_affinity_flags as (
    select
        customer_id,
        -- Summer Rosé (May-August)
        max(case when color = 'Rosé' and order_month in (5, 6, 7, 8) then 1 else 0 end) as has_summer_rose,
        -- Summer Whites (May-August)
        max(case when color = 'White' and order_month in (5, 6, 7, 8) then 1 else 0 end) as has_summer_whites,
        -- Holiday/Winter Reds (October-February)
        max(case when color = 'Red' and order_month in (10, 11, 12, 1, 2) then 1 else 0 end) as has_winter_reds,
        -- Holiday Sparkling (November-December, January)
        max(case when color = 'Sparkling' and order_month in (11, 12, 1) then 1 else 0 end) as has_holiday_sparkling,
        -- Valentine's Sparkling (February)
        max(case when color = 'Sparkling' and order_month = 2 then 1 else 0 end) as has_valentines_sparkling,
        -- Spring Whites (March-May)
        max(case when color = 'White' and order_month in (3, 4, 5) then 1 else 0 end) as has_spring_whites,
        -- Fall Reds (September-November)
        max(case when color = 'Red' and order_month in (9, 10, 11) then 1 else 0 end) as has_fall_reds,
        -- Year-round consistency (purchases across all seasons)
        count(distinct case when order_month in (12, 1, 2) then order_month end) > 0 
            and count(distinct case when order_month in (3, 4, 5) then order_month end) > 0
            and count(distinct case when order_month in (6, 7, 8) then order_month end) > 0
            and count(distinct case when order_month in (9, 10, 11) then order_month end) > 0 as has_year_round
    from seasonal_patterns
    where color != 'Unknown'
    group by 1
),

-- RFM quantiles (12-month window for F/M)
rfm as (
    select
        oim.customer_id,
        oim.recency_days,
        oim.orders_12mo as frequency_12mo,
        oim.revenue_12mo as monetary_12mo,
        6 - ntile(5) over (order by coalesce(oim.recency_days, 999999)) as r_score,
        ntile(5) over (order by coalesce(oim.orders_12mo, 0)) as f_score,
        ntile(5) over (order by coalesce(oim.revenue_12mo, 0)) as m_score
    from order_item_metrics oim
),

customer_segments as (
    select 
        cb.*,
        rm.state as ruca_state,
        rm.place_name,
        rm.primary_ruca_code,
        rm.secondary_ruca_code,
        rm.ruca_category,
        rm.ruca_description,
        -- Customer value segments
        case 
            when cb.lifetime_value_dollars >= 3000 then 'High Value'
            when cb.lifetime_value_dollars >= 1750 then 'Medium Value'
            when cb.lifetime_value_dollars > 0 then 'Low Value'
            else 'No Purchase History'
        end as value_segment,
        -- Order frequency segments
        case 
            when cb.order_count >= 10 then 'Frequent Buyer'
            when cb.order_count >= 5 then 'Regular Buyer'
            when cb.order_count >= 2 then 'Occasional Buyer'
            when cb.order_count = 1 then 'One-time Buyer'
            else 'No Orders'
        end as frequency_segment,
        -- Club membership status
        case 
            when cb.is_active_club_member then 'Active Member'
            else 'Non-Member'
        end as membership_segment,
        -- Geographic market size
        case 
            when rm.ruca_category = 'Metropolitan' then 'Major Metro'
            when rm.ruca_category = 'Micropolitan' then 'Mid-Size Market'
            when rm.ruca_category = 'Small Town' then 'Small Market'
            when rm.ruca_category = 'Rural' then 'Rural Market'
            else 'Unknown Market'
        end as market_size,

        -- Order item metrics
        oim.last_order_date,
        oim.first_order_date,
        oim.recency_days,
        oim.orders_12mo,
        oim.revenue_12mo,
        oim.aov_12mo,
        oim.avg_unit_price_all_time,
        oim.total_orders,
        oim.lifetime_revenue,
        oim.total_items_purchased,
        oim.avg_items_per_order,

        -- Order level metrics (from fct_order)
        olm.online_orders,
        olm.pos_orders,
        olm.inbound_orders,
        olm.club_orders,
        olm.pickup_orders,
        olm.shipping_orders,
        olm.carry_out_orders,
        olm.paid_orders,
        olm.last_order_date_from_orders,
        
        -- Sales associate metrics
        sam.distinct_sales_associates,
        sam.primary_sales_associate,
        sam.orders_with_associate,

        -- Club membership details
        cmd.total_memberships,
        cmd.first_membership_signup,
        cmd.last_membership_signup,
        cmd.last_membership_cancellation,
        cmd.active_memberships,
        cmd.cancelled_memberships,
        cmd.max_shipments,
        cmd.most_recent_signup_date,
        cmd.days_since_first_signup,

        -- Preference signals
        ps.price_tier_preference,
        ps.luxury_share,
        ps.premium_share,
        ps.mid_share,
        ps.value_share,
        ps.top_varietal,
        ps.top_varietal_share,
        ps.top_color,
        ps.top_color_share,

        case when s.rn = 1 then s.order_month end as favorite_purchase_month,

        rfm.r_score,
        rfm.f_score,
        rfm.m_score,
        (rfm.r_score + rfm.f_score + rfm.m_score) as rfm_score,

        -- Derived segments
        case 
            when oim.recency_days <= 60 and oim.orders_12mo >= 4 then 'Loyal'
            when oim.recency_days <= 60 and oim.orders_12mo between 2 and 3 then 'Growth'
            when oim.recency_days <= 60 and oim.orders_12mo <= 1 then 'New'
            when oim.recency_days between 61 and 180 then 'Warming'
            when oim.recency_days between 181 and 365 then 'At Risk'
            else 'Inactive'
        end as lifecycle_stage,

        -- Seasonal affinity: concatenate all matching categories
        trim(both ', ' from
            concat_ws(', ',
                case when saf.has_summer_rose = 1 then 'Summer Rosé' end,
                case when saf.has_summer_whites = 1 then 'Summer Whites' end,
                case when saf.has_winter_reds = 1 then 'Holiday/Winter Reds' end,
                case when saf.has_holiday_sparkling = 1 then 'Holiday Sparkling' end,
                case when saf.has_valentines_sparkling = 1 then 'Valentine''s Sparkling' end,
                case when saf.has_spring_whites = 1 then 'Spring Whites' end,
                case when saf.has_fall_reds = 1 then 'Fall Reds' end,
                case when saf.has_year_round = true then 'Year-Round Buyer' end
            )
        ) as seasonal_affinity,

        -- Channel preference
        case
            when olm.online_orders > coalesce(olm.pos_orders, 0) + coalesce(olm.inbound_orders, 0) + coalesce(olm.club_orders, 0) then 'Online Preferrer'
            when olm.pos_orders > coalesce(olm.online_orders, 0) + coalesce(olm.inbound_orders, 0) + coalesce(olm.club_orders, 0) then 'POS Preferrer'
            when olm.inbound_orders > coalesce(olm.online_orders, 0) + coalesce(olm.pos_orders, 0) + coalesce(olm.club_orders, 0) then 'Inbound Preferrer'
            when olm.club_orders > coalesce(olm.online_orders, 0) + coalesce(olm.pos_orders, 0) + coalesce(olm.inbound_orders, 0) then 'Club Preferrer'
            when olm.online_orders > 0 or olm.pos_orders > 0 or olm.inbound_orders > 0 or olm.club_orders > 0 then 'Multi-Channel'
            else 'Unknown'
        end as channel_preference,

        -- Delivery preference
        case
            when olm.pickup_orders > coalesce(olm.shipping_orders, 0) + coalesce(olm.carry_out_orders, 0) then 'Pickup Preferrer'
            when olm.shipping_orders > coalesce(olm.pickup_orders, 0) + coalesce(olm.carry_out_orders, 0) then 'Shipping Preferrer'
            when olm.carry_out_orders > coalesce(olm.pickup_orders, 0) + coalesce(olm.shipping_orders, 0) then 'Carry Out Preferrer'
            when olm.pickup_orders > 0 or olm.shipping_orders > 0 or olm.carry_out_orders > 0 then 'Multi-Delivery'
            else 'Unknown'
        end as delivery_preference,

        -- Club engagement
        case
            when cmd.active_memberships > 0 then 'Active Club Member'
            when cmd.cancelled_memberships > 0 then 'Former Club Member'
            when cb.is_active_club_member then 'Club Member (Legacy Flag)'
            else 'Non-Member'
        end as club_engagement_status
    from customer_base cb
    left join ruca_mapping rm 
        on substring(cb.postal_code, 1, 5) = rm.postal_code
    left join order_item_metrics oim on cb.customer_id = oim.customer_id
    left join order_level_metrics olm on cb.customer_id = olm.customer_id
    left join sales_associate_metrics sam on cb.customer_id = sam.customer_id
    left join club_membership_details cmd on cb.customer_id = cmd.customer_id
    left join preference_signals ps on cb.customer_id = ps.customer_id
    left join seasonality s on cb.customer_id = s.customer_id and s.rn = 1
    left join seasonal_affinity_flags saf on cb.customer_id = saf.customer_id
    left join rfm on cb.customer_id = rfm.customer_id
)

select 
    customer_id,
    first_name,
    last_name,
    city,
    state_code,
    postal_code,
    country_code,
    primary_email,
    email_mkt_status,
    has_account,
    order_count,
    lifetime_value_dollars,
    lifetime_gross_profit_dollars,
    is_active_club_member,
    acquisition_channel,
    customer_tags,
    -- RUCA data
    ruca_state,
    place_name,
    primary_ruca_code,
    secondary_ruca_code,
    ruca_category,
    ruca_description,
    -- Segmentation
    value_segment,
    frequency_segment,
    membership_segment,
    market_size,
    -- Combined segments
    concat(value_segment, ' - ', frequency_segment) as combined_value_frequency_segment,
    concat(market_size, ' - ', membership_segment) as combined_geographic_membership_segment,
    -- RFM and lifecycle
    last_order_date,
    first_order_date,
    recency_days,
    orders_12mo,
    revenue_12mo,
    aov_12mo,
    r_score,
    f_score,
    m_score,
    rfm_score,
    lifecycle_stage,
    -- Order item metrics
    total_orders,
    lifetime_revenue,
    total_items_purchased,
    avg_items_per_order,
    -- Price
    avg_unit_price_all_time,
    price_tier_preference,
    luxury_share,
    premium_share,
    mid_share,
    value_share,
    -- Order level metrics (channel and delivery)
    online_orders,
    pos_orders,
    inbound_orders,
    club_orders,
    pickup_orders,
    shipping_orders,
    carry_out_orders,
    paid_orders,
    last_order_date_from_orders,
    channel_preference,
    delivery_preference,
    -- Sales associate
    distinct_sales_associates,
    primary_sales_associate,
    orders_with_associate,
    -- Club membership
    total_memberships,
    first_membership_signup,
    last_membership_signup,
    last_membership_cancellation,
    active_memberships,
    cancelled_memberships,
    max_shipments,
    most_recent_signup_date,
    days_since_first_signup,
    club_engagement_status,
    -- Preference and seasonality
    top_varietal,
    top_varietal_share,
    top_color,
    top_color_share,
    favorite_purchase_month,
    seasonal_affinity,
    created_at,
    updated_at
from customer_segments
order by lifetime_value_dollars desc, order_count desc
