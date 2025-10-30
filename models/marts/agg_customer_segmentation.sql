{{ config(materialized='table') }}

with customer_segments as (
    select 
        cs.ruca_category,
        cs.market_size,
        cs.value_segment,
        cs.frequency_segment,
        cs.membership_segment,
        cs.combined_value_frequency_segment,
        cs.combined_geographic_membership_segment,
        cs.acquisition_channel,
        cs.state_code,
        count(*) as customer_count,
        sum(cs.lifetime_value_dollars) as total_lifetime_value,
        avg(cs.lifetime_value_dollars) as avg_lifetime_value,
        sum(cs.order_count) as total_orders,
        avg(cs.order_count) as avg_orders_per_customer,
        count(case when cs.is_active_club_member then 1 end) as active_club_members,
        count(case when cs.email_mkt_status = 'subscribed' then 1 end) as email_subscribers,
        count(case when cs.has_account then 1 end) as account_holders
    from {{ ref('dim_customer_segmentation') }} cs
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

segment_metrics as (
    select 
        *,
        -- Calculate percentages
        round(100.0 * active_club_members / customer_count, 2) as club_membership_rate,
        round(100.0 * email_subscribers / customer_count, 2) as email_subscription_rate,
        round(100.0 * account_holders / customer_count, 2) as account_creation_rate,
        -- Calculate value per customer
        round(total_lifetime_value / customer_count, 2) as value_per_customer,
        round(total_orders::numeric / customer_count, 2) as orders_per_customer
    from customer_segments
)

select 
    ruca_category,
    market_size,
    value_segment,
    frequency_segment,
    membership_segment,
    combined_value_frequency_segment,
    combined_geographic_membership_segment,
    acquisition_channel,
    state_code,
    customer_count,
    total_lifetime_value,
    avg_lifetime_value,
    value_per_customer,
    total_orders,
    avg_orders_per_customer,
    orders_per_customer,
    active_club_members,
    club_membership_rate,
    email_subscribers,
    email_subscription_rate,
    account_holders,
    account_creation_rate
from segment_metrics
order by total_lifetime_value desc, customer_count desc
