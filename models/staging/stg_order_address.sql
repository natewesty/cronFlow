{{ config(materialized='view') }}

with base as (
    select order_id, _order_json as o
    from {{ ref('stg_order') }}
),

addrs as (
    /* ---------- Bill‑to ---------- */
    select order_id,
           'bill_to'           as address_type,
           coalesce(o->'billTo',                o->'billToCustomerAddress') as a
    from base

    union all
    /* ---------- Ship‑to ---------- */
    select order_id,
           'ship_to',
           coalesce(o->'shipTo',                o->'shipToCustomerAddress')
    from base

    union all
    /* ---------- Carry‑out / Pickup ---------- */
    select order_id,
           'carry_out',
           o->'carryOut'
    from base
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
        coalesce(a->>'address', a->>'address1') as line1,
        a->>'address2'                       as line2,
        a->>'city'                           as city,
        a->>'stateCode'                      as state,
        a->>'zipCode'                        as postal_code,
        a->>'countryCode'                    as country_code
    from addrs
    where a is not null                     -- ignore missing blocks
)

select * from norm;
