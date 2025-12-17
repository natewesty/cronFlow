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
        -- Convert UTC timestamps to Pacific Time
        ((data->>'orderSubmittedDate')::timestamptz AT TIME ZONE 'America/Los_Angeles') as submitted_at,
        ((data->>'orderPaidDate')::timestamptz AT TIME ZONE 'America/Los_Angeles') as paid_at,
        ((data->>'orderFulfilledDate')::timestamptz AT TIME ZONE 'America/Los_Angeles') as fulfilled_at,

        /* ───── statuses & refs ───── */
        data->>'channel'                         as channel,
        case
            when data->>'shipTo' is null then null
            else data->'shipTo'->>'stateCode'
        end as state_code,
        data->>'orderDeliveryMethod'             as delivery_method,
        data->>'externalOrderVendor'             as external_order_vendor,
        data->>'refundOrderId'                    as refund_order_id,
        (
            select (linked_order->>'orderId')::uuid
            from jsonb_array_elements(data->'linkedOrders') as linked_order
            where linked_order->>'purchaseType' = 'Refund'
            limit 1
        ) as linked_order_id,
        (
            select linked_order->>'purchaseType'
            from jsonb_array_elements(data->'linkedOrders') as linked_order
            where linked_order->>'purchaseType' = 'Refund'
            limit 1
        ) as linked_order_purchase_type,
        data->>'paymentStatus'                   as payment_status,
        data->>'fulfillmentStatus'               as fulfillment_status,
        (data->'fulfillments'->0->>'id')::uuid   as fulfillment_id,
        data->>'shippingStatus'                  as shipping_status,
        data->>'salesAttributionCode'            as sales_attribution_code,
        (data->>'customerId')::uuid              as customer_id,
        (data->>'posProfileId')::uuid            as pos_profile_id,
        data->>'taxSaleType'                     as tax_sale_type,

        /* ───── sales associate ───── */
        (data->'salesAssociate'->>'accountId')::uuid as sales_associate_id,
        data->'salesAssociate'->>'name' as sales_associate,

        /* ───── money (still in cents) ───── */
        coalesce((data->>'subTotal')::bigint,0)       as sub_total_cents,
        coalesce((data->>'shipTotal')::bigint,0)      as ship_total_cents,
        coalesce((data->>'taxTotal')::bigint,0)       as tax_total_cents,
        coalesce((data->>'tipTotal')::bigint,0)       as tip_total_cents,
        coalesce((data->>'total')::bigint,0)          as total_cents,
        coalesce((data->>'totalAfterTip')::bigint,0)  as total_after_tip_cents,

        /* ───── metadata fields ───── */
        data->'metaData'->>'tasting-lounge'                    as tasting_lounge,
        data->'metaData'->>'event-fee-or-wine'                 as event_fee_or_wine,
        data->'metaData'->>'event-specific-sale'               as event_specific_sale,
        data->'metaData'->>'event-revenue-relization-date'    as event_revenue_realization_date,

        /* ───── bookkeeping ───── */
        -- Convert UTC timestamps to Pacific Time
        ((data->>'createdAt')::timestamptz AT TIME ZONE 'America/Los_Angeles') as created_at,
        ((data->>'updatedAt')::timestamptz AT TIME ZONE 'America/Los_Angeles') as updated_at,
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
