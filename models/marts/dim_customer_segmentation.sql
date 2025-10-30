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
            when cb.lifetime_value_dollars >= 1000 then 'High Value'
            when cb.lifetime_value_dollars >= 500 then 'Medium Value'
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
        end as market_size
    from customer_base cb
    left join ruca_mapping rm 
        on substring(cb.postal_code, 1, 5) = rm.postal_code
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
    created_at,
    updated_at
from customer_segments
order by lifetime_value_dollars desc, order_count desc
