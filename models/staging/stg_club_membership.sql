{{ config(
    materialized          = 'incremental',
    unique_key            = 'membership_id',
    incremental_strategy  = 'merge',
    on_schema_change      = 'sync_all_columns'
) }}

with src as (
    select
        (m->>'id')::uuid                        as membership_id,
        m->>'status'                            as status,
        (m->>'customerId')::uuid                as customer_id,
        (m->>'clubId')::uuid                    as club_id,
        m->>'clubType'                          as club_type,

        -- addresses & fulfillment
        (m->>'billToCustomerAddressId')::uuid   as bill_to_address_id,
        (m->>'shipToCustomerAddressId')::uuid   as ship_to_address_id,
        (m->>'pickupInventoryLocationId')::uuid as pickup_inventory_location_id,
        m->>'orderDeliveryMethod'               as delivery_method,
        (m->>'customerCreditCardId')::uuid      as customer_credit_card_id,

        -- lifecycle & status dates
        (m->>'signupDate')::timestamptz         as signup_at,
        (m->>'cancelDate')::timestamptz         as cancel_at,
        (m->>'autoRenewalConsentDate')::timestamptz as auto_renewal_consent_at,
        (m->>'lastProcessedDate')::timestamptz  as last_processed_at,

        -- cancellation detail
        m->>'cancellationReason'                as cancellation_reason,
        m->>'cancellationComments'              as cancellation_comments,

        -- operational metrics
        coalesce((m->>'currentNumberOfShipments')::int,0) as current_shipments,
        m->>'acquisitionChannel'                as acquisition_channel,

        -- misc text fields
        m->>'giftMessage'                       as gift_message,
        m->>'shippingInstructions'              as shipping_instructions,

        -- bookkeeping
        (m->>'createdAt')::timestamptz          as created_at,
        (m->>'updatedAt')::timestamptz          as updated_at,
        r.load_ts,
        m                                        as _membership_json
    from {{ source('raw','raw_club_membership') }} r
    cross join lateral jsonb_array_elements(r.data->'clubMemberships') m
),

dedup as (
    select
        *
      , row_number() over (
            partition by membership_id
            order by updated_at desc, load_ts desc
        ) as rn
    from src
)

