{{ config(materialized='view') }}

with base as (
    select order_id, _order_json as o
    from {{ ref('stg_order') }}
),

addrs as (
    /* ---------- Bill‑to ---------- */
    select order_id,
           'bill_to'                        as address_type,
           o->'billTo'                      as a
    from base
    where o->'billTo' is not null 
      and o->'billTo' != 'null'::jsonb
      and o->'billTo' != '{}'::jsonb

    union all
    /* ---------- Ship‑to ---------- */
    select order_id,
           'ship_to',
           o->'shipTo'
    from base
    where o->'shipTo' is not null
      and o->'shipTo' != 'null'::jsonb
      and o->'shipTo' != '{}'::jsonb
),

norm as (
    select
        order_id,
        address_type,
        (a->>'id')::uuid                     as address_id,
        a->>'firstName'                      as first_name,
        a->>'lastName'                       as last_name,
        a->>'company'                        as company,
        a->>'phone'                          as phone,
        a->>'address'                        as line1,
        a->>'address2'                       as line2,
        a->>'city'                           as city,
        a->>'stateCode'                      as state,
        a->>'zipCode'                        as postal_code,
        a->>'countryCode'                    as country_code
    from addrs
    where a is not null
)

select * from norm
