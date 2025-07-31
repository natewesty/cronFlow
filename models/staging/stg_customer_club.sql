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
    (cl->>'signupDate')::timestamptz as signup_at,
    (cl->>'cancelDate')::timestamptz as cancel_at,
    updated_at
from base
cross join lateral jsonb_array_elements(c->'clubs') cl
