{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_customer') }}
),

emails as (
    select customer_id, min(email) as primary_email
    from {{ ref('stg_customer_email') }}
    group by 1
),

tags as (
    select customer_id,
           string_agg(tag_title, ', ') as customer_tags
    from {{ ref('stg_customer_tag') }}
    group by 1
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.birth_date,
    c.city,
    c.state_code,
    c.postal_code,
    c.country_code,
    c.email_mkt_status,
    coalesce(e.primary_email,'')      as primary_email,
    c.has_account,
    c.order_count,
    c.lifetime_value_cents / 100.0    as lifetime_value_dollars,
    c.gross_profit_cents  / 100.0     as lifetime_gross_profit_dollars,
    c.is_active_club_member,
    c.acquisition_channel,
    t.customer_tags,
    c.created_at,
    c.updated_at
from base c
left join emails e using (customer_id)
left join tags   t using (customer_id)
