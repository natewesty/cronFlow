{{ config(materialized='view') }}

with base as (
    select customer_id, _customer_json as c, updated_at
    from {{ ref('stg_customer') }}
)

select
    customer_id,
    (cl->>'clubMembershipId')::uuid  as club_membership_id,
    (cl->>'clubId')::uuid            as club_id,
    cl->>'clubTitle'                 as club_title,
    -- Convert UTC timestamps to Pacific Time
    (cl->>'signupDate')::timestamptz AT TIME ZONE 'America/Los_Angeles' as signup_at,
    (cl->>'cancelDate')::timestamptz AT TIME ZONE 'America/Los_Angeles' as cancel_at,
    updated_at
from base
cross join lateral jsonb_array_elements(c->'clubs') cl
