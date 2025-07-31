{{ config(
    materialized        = 'incremental',
    unique_key          = 'membership_id',
    incremental_strategy= 'merge',
    on_schema_change    = 'sync_all_columns'
) }}

with src as (

    select
        (data->>'id')::uuid                     as membership_id,
        data->>'status'                         as status,
        (data->>'customerId')::uuid             as customer_id,
        (data->>'clubId')::uuid                 as club_id,
        data->>'clubType'                       as club_type,

        /* addresses & fulfillment */
        (data->>'billToCustomerAddressId')::uuid   as bill_to_address_id,
        (data->>'shipToCustomerAddressId')::uuid   as ship_to_address_id,
        (data->>'pickupInventoryLocationId')::uuid as pickup_inventory_location_id,
        data->>'orderDeliveryMethod'               as delivery_method,
        (data->>'customerCreditCardId')::uuid      as customer_credit_card_id,

        /* lifecycle */
        -- Convert UTC timestamps to Pacific Time
        (data->>'signupDate')::timestamptz AT TIME ZONE 'America/Los_Angeles' as signup_at,
        (data->>'cancelDate')::timestamptz AT TIME ZONE 'America/Los_Angeles' as cancel_at,
        (data->>'autoRenewalConsentDate')::timestamptz AT TIME ZONE 'America/Los_Angeles' as auto_renewal_consent_at,
        (data->>'lastProcessedDate')::timestamptz AT TIME ZONE 'America/Los_Angeles' as last_processed_at,

        /* cancellation */
        data->>'cancellationReason'                as cancellation_reason,
        data->>'cancellationComments'              as cancellation_comments,

        /* metrics & misc */
        coalesce((data->>'currentNumberOfShipments')::int, 0) as current_shipments,
        data->>'acquisitionChannel'                as acquisition_channel,
        data->>'giftMessage'                       as gift_message,
        data->>'shippingInstructions'              as shipping_instructions,

        /* bookkeeping */
        -- Convert UTC timestamps to Pacific Time
        (data->>'createdAt')::timestamptz AT TIME ZONE 'America/Los_Angeles' as created_at,
        (data->>'updatedAt')::timestamptz AT TIME ZONE 'America/Los_Angeles' as updated_at,
        coalesce(last_processed_at, current_timestamp)        as load_ts,
        data                                       as _membership_json

    from {{ source('raw','raw_club_membership') }}

    {% if is_incremental() %}
      where last_processed_at >
            (select coalesce(max(load_ts), date '2000-01-01') from {{ this }})
    {% endif %}
),

dedup as (
    select *
         , row_number() over (
               partition by membership_id
               order by updated_at desc, load_ts desc
           ) as rn
    from src
)

select * from dedup
where rn = 1
