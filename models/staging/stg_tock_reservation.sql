{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from {{ source('raw', 'raw_tock_reservation') }}
),

parsed_data as (
    select
        id::varchar as reservation_id,
        data->>'id'::varchar as tock_reservation_id,
        
        -- Business information
        data->'business'->>'id'::int as business_id,
        data->'business'->>'name'::varchar as business_name,
        data->'business'->>'domainName'::varchar as business_domain_name,
        data->'business'->>'locale'::varchar as business_locale,
        data->'business'->>'currencyCode'::varchar as business_currency_code,
        data->'business'->>'timeZone'::varchar as business_time_zone,
        
        -- Reservation details
        data->>'dateTime'::varchar as reservation_datetime,
        data->>'partySize'::int as party_size,
        data->>'partyState'::varchar as party_state,
        data->>'sequenceId'::int as sequence_id,
        data->>'confirmationCode'::varchar as confirmation_code,
        data->>'serverName'::varchar as server_name,
        
        -- Experience information
        data->'experience'->>'id'::int as experience_id,
        data->'experience'->>'name'::varchar as experience_name,
        data->'experience'->>'amountCents'::int as experience_amount_cents,
        data->'experience'->>'variety'::varchar as experience_variety,
        
        -- Pricing information
        data->>'subtotalCents'::int as subtotal_cents,
        data->>'taxRate'::float as tax_rate,
        data->>'taxCents'::int as tax_cents,
        data->>'serviceChargeRate'::float as service_charge_rate,
        data->>'serviceChargeCents'::int as service_charge_cents,
        data->>'selectedGratuityRate'::float as selected_gratuity_rate,
        data->>'gratuityCents'::int as gratuity_cents,
        data->>'eventFeeRate'::float as event_fee_rate,
        data->>'eventFeeCents'::int as event_fee_cents,
        data->>'customFeeRate'::float as custom_fee_rate,
        data->>'customFeeCents'::int as custom_fee_cents,
        data->>'customFeeName'::varchar as custom_fee_name,
        data->>'totalPriceCents'::int as total_price_cents,
        data->>'netAmountPaidCents'::int as net_amount_paid_cents,
        data->>'amountDueCents'::int as amount_due_cents,
        
        -- Owner patron information
        data->'ownerPatron'->>'id'::int as owner_patron_id,
        data->'ownerPatron'->>'email'::varchar as owner_patron_email,
        data->'ownerPatron'->>'firstName'::varchar as owner_patron_first_name,
        data->'ownerPatron'->>'lastName'::varchar as owner_patron_last_name,
        data->'ownerPatron'->>'phone'::varchar as owner_patron_phone,
        data->'ownerPatron'->>'phoneCountryCode'::varchar as owner_patron_phone_country_code,
        data->'ownerPatron'->>'zipCode'::varchar as owner_patron_zip_code,
        data->'ownerPatron'->>'imageUrl'::varchar as owner_patron_image_url,
        data->'ownerPatron'->>'isoCountryCode'::varchar as owner_patron_iso_country_code,
        
        -- Diner patron information
        data->'dinerPatron'->>'id'::int as diner_patron_id,
        data->'dinerPatron'->>'email'::varchar as diner_patron_email,
        data->'dinerPatron'->>'firstName'::varchar as diner_patron_first_name,
        data->'dinerPatron'->>'lastName'::varchar as diner_patron_last_name,
        data->'dinerPatron'->>'phone'::varchar as diner_patron_phone,
        data->'dinerPatron'->>'phoneCountryCode'::varchar as diner_patron_phone_country_code,
        data->'dinerPatron'->>'zipCode'::varchar as diner_patron_zip_code,
        data->'dinerPatron'->>'imageUrl'::varchar as diner_patron_image_url,
        data->'dinerPatron'->>'isoCountryCode'::varchar as diner_patron_iso_country_code,
        
        -- Status flags
        data->>'transferredOut'::boolean as transferred_out,
        data->>'isCancelled'::boolean as is_cancelled,
        
        -- Timestamps
        to_timestamp((data->>'createdTimestamp')::bigint / 1000) as created_at,
        to_timestamp((data->>'lastUpdatedTimestamp')::bigint / 1000) as last_updated_at,
        to_timestamp((data->>'serviceDateTimestamp')::bigint / 1000) as service_date,
        
        -- Version tracking
        data->>'versionId'::bigint as version_id,
        
        -- Arrays - extract counts and first elements
        jsonb_array_length(data->'option') as option_count,
        jsonb_array_length(data->'fee') as fee_count,
        jsonb_array_length(data->'customCharge') as custom_charge_count,
        jsonb_array_length(data->'keyValue') as key_value_count,
        jsonb_array_length(data->'discount') as discount_count,
        jsonb_array_length(data->'visitFeedback') as visit_feedback_count,
        jsonb_array_length(data->'visitTag') as visit_tag_count,
        jsonb_array_length(data->'payment') as payment_count,
        jsonb_array_length(data->'refund') as refund_count,
        jsonb_array_length(data->'note') as note_count,
        jsonb_array_length(data->'question') as question_count,
        jsonb_array_length(data->'table') as table_count,
        
        -- Extract key-value pairs for origin tracking
        case 
            when jsonb_array_length(data->'keyValue') > 0 
            then data->'keyValue'->0->>'attribute'::varchar 
            else null 
        end as primary_key_attribute,
        case 
            when jsonb_array_length(data->'keyValue') > 0 
            then data->'keyValue'->0->>'attributeValue'::varchar 
            else null 
        end as primary_key_value,
        
        -- Extract notes
        data->'note' as notes,
        
        -- Extract payments
        data->'payment' as payments,
        
        -- Extract refunds
        data->'refund' as refunds,
        
        -- Extract discounts
        data->'discount' as discounts,
        
        -- Store full arrays for complex analysis
        data->'option' as options,
        data->'fee' as fees,
        data->'customCharge' as custom_charges,
        data->'keyValue' as key_values,
        data->'visitFeedback' as visit_feedbacks,
        data->'visitTag' as visit_tags,
        data->'question' as questions,
        data->'table' as tables,
        
        -- Airbyte metadata
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_tock_reservation_hashid,
        last_processed_at
        
    from source_data
)

select * from parsed_data
