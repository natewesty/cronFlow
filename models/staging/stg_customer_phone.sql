{{ config(materialized='view') }}

with base as (
    select customer_id, _customer_json as c, updated_at
    from {{ ref('stg_customer') }}
)

select
    customer_id,
    (p->>'id')::uuid  as phone_id,
    p->>'phone'       as phone,
    updated_at
from base
cross join lateral jsonb_array_elements(c->'phones') p