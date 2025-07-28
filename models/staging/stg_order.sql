{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with src as (

  select
      (o->>'id')::uuid                        as order_id,
      (o->>'orderNumber')::bigint             as order_number,
      (o->>'orderSubmittedDate')::timestamptz as submitted_at,
      (o->>'orderPaidDate')::timestamptz      as paid_at,
      (o->>'orderFulfilledDate')::timestamptz as fulfilled_at,
      o->>'channel'                           as channel,
      o->>'orderDeliveryMethod'               as delivery_method,
      o->>'paymentStatus'                     as payment_status,
      o->>'fulfillmentStatus'                 as fulfillment_status,
      o->>'shippingStatus'                    as shipping_status,
      o->>'salesAttributeCode'                as sales_attribute_code,
      (o->>'customerId')::uuid                as customer_id,
      (o->>'posProfileId')::uuid              as pos_profile_id,
      o->>'taxSaleType'                       as tax_sale_type,

      coalesce((o->>'subTotal')::bigint,0)      as sub_total_cents,
      coalesce((o->>'shipTotal')::bigint,0)     as ship_total_cents,
      coalesce((o->>'taxTotal')::bigint,0)      as tax_total_cents,
      coalesce((o->>'tipTotal')::bigint,0)      as tip_total_cents,
      coalesce((o->>'total')::bigint,0)         as total_cents,
      coalesce((o->>'totalAfterTip')::bigint,0) as total_after_tip_cents,

      (o->>'createdAt')::timestamptz          as created_at,
      (o->>'updatedAt')::timestamptz          as updated_at,
      coalesce(r.last_processed, current_timestamp) as load_ts,
      o                                       as _order_json

  from {{ source('raw', 'raw_order') }} r
  cross join lateral jsonb_array_elements(r.data->'orders') o
),

dedup as (
  select *
       , row_number() over (
           partition by order_id
           order by updated_at desc, load_ts desc
         ) as rn
  from src
)

select * from dedup
where rn = 1

{% if is_incremental() %}
  and updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
        from {{ this }}
  )
{% endif %}
