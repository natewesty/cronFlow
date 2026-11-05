{{ config(materialized='view') }}

-- Orders that must not count anywhere in reporting: every order placed by a
-- customer listed in the dim_excluded_customer seed, inside that row's window.
-- This view is the single definition of "excluded order". stg_order filters on
-- it, and each incremental order model (stg_order, stg_order_item, fct_order,
-- fct_order_item) runs a post-hook delete against it so rows already loaded
-- before the seed row existed are removed on the next run.
--
-- Built from raw_order rather than stg_order so excluded orders stay visible
-- here for audit after they disappear from the staging tables.

with raw_orders as (

    select
        (data->>'id')::uuid                 as order_id,
        (data->>'orderNumber')::bigint      as order_number,
        (data->>'customerId')::uuid         as customer_id,
        data->>'channel'                    as channel,
        data->>'paymentStatus'              as payment_status,
        (
            coalesce(
                (data->>'orderPaidDate')::timestamptz,
                (data->>'orderSubmittedDate')::timestamptz,
                (data->>'createdAt')::timestamptz
            ) at time zone 'America/Los_Angeles'
        )                                   as order_at,
        coalesce((data->>'subTotal')::bigint, 0) / 100.0 as subtotal,
        coalesce((data->>'total')::bigint, 0)    / 100.0 as order_total,
        data->'salesAssociate'->>'name'     as sales_associate
    from {{ source('raw', 'raw_order') }}
    where data->>'customerId' in (
        select customer_id::text from {{ ref('dim_excluded_customer') }}
    )

)

select distinct on (o.order_id)
    o.order_id,
    o.order_number,
    o.customer_id,
    x.customer_name,
    o.order_at,
    o.channel,
    o.payment_status,
    o.subtotal,
    o.order_total,
    o.sales_associate,
    x.reason
from raw_orders o
join {{ ref('dim_excluded_customer') }} x
  on x.customer_id = o.customer_id
where date(o.order_at) >= x.exclude_from
  and (x.exclude_to is null or date(o.order_at) <= x.exclude_to)
order by o.order_id
