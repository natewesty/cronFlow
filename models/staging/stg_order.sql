{{ config(
    materialized        = 'incremental',
    unique_key          = 'order_id',
    incremental_strategy= 'merge',
    on_schema_change    = 'sync_all_columns'
) }}

with src as (

    select
        /* ───── identifiers & dates ───── */
        (data->>'id')::uuid                      as order_id,
        (data->>'orderNumber')::bigint           as order_number,
        (data->>'orderSubmittedDate')::timestamptz   as submitted_at,
        (data->>'orderPaidDate')::timestamptz        as paid_at,
        (data->>'orderFulfilledDate')::timestamptz   as fulfilled_at,

        /* ───── statuses & refs ───── */
        data->>'channel'                         as channel,
        data->>'orderDeliveryMethod'             as delivery_method,
        data->>'paymentStatus'                   as payment_status,
        data->>'fulfillmentStatus'               as fulfillment_status,
        data->>'shippingStatus'                  as shipping_status,
        data->>'salesAttributionCode'            as sales_attribution_code,
        (data->>'customerId')::uuid              as customer_id,
        (data->>'posProfileId')::uuid            as pos_profile_id,
        data->>'taxSaleType'                     as tax_sale_type,

        /* ───── money (still in cents) ───── */
        coalesce((data->>'subTotal')::bigint,0)       as sub_total_cents,
        coalesce((data->>'shipTotal')::bigint,0)      as ship_total_cents,
        coalesce((data->>'taxTotal')::bigint,0)       as tax_total_cents,
        coalesce((data->>'tipTotal')::bigint,0)       as tip_total_cents,
        coalesce((data->>'total')::bigint,0)          as total_cents,
        coalesce((data->>'totalAfterTip')::bigint,0)  as total_after_tip_cents,

        /* ───── bookkeeping ───── */
        (data->>'createdAt')::timestamptz         as created_at,
        (data->>'updatedAt')::timestamptz         as updated_at,
        coalesce(last_processed_at, current_timestamp) as load_ts,
        data                                      as _order_json

    from {{ source('raw', 'raw_order') }}

    {% if is_incremental() %}
      -- only pull rows ingested since the most‑recent load_ts we processed
      where last_processed_at >
            (select coalesce(max(load_ts), date '2000-01-01') from {{ this }})
    {% endif %}
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
