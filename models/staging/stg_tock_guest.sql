{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from {{ source('raw', 'raw_tock_guest') }}
),

parsed_data as (
    select
        id::varchar as guest_id,
        data->>'id' as tock_guest_id,
        data->'patron'->>'id' as patron_id,
        data->'patron'->>'email' as email,
        data->'patron'->>'firstName' as first_name,
        data->'patron'->>'lastName' as last_name,
        data->'patron'->>'phone' as phone,
        data->'patron'->>'phoneCountryCode' as phone_country_code,
        data->'patron'->>'zipCode' as zip_code,
        data->'patron'->>'isoCountryCode' as iso_country_code,
        data->'address'->>'country' as address_country,
        
        -- Extract phone array data
        jsonb_array_length(data->'phone') as phone_count,
        data->'phone'->0->>'phone' as primary_phone,
        data->'phone'->0->>'phoneCountryCode' as primary_phone_country_code,
        
        -- Extract day array data (birthdays, anniversaries, etc.)
        jsonb_array_length(data->'day') as day_count,
        case 
            when jsonb_array_length(data->'day') > 0 
            then data->'day'->0->>'type'
            else null 
        end as primary_day_type,
        case 
            when jsonb_array_length(data->'day') > 0 
            then (data->'day'->0->>'day')::int 
            else null 
        end as primary_day,
        case 
            when jsonb_array_length(data->'day') > 0 
            then (data->'day'->0->>'month')::int 
            else null 
        end as primary_month,
        case 
            when jsonb_array_length(data->'day') > 0 
            then (data->'day'->0->>'year')::int 
            else null 
        end as primary_year,
        
        -- Extract link array data
        jsonb_array_length(data->'link') as link_count,
        case 
            when jsonb_array_length(data->'link') > 0 
            then data->'link'->0->>'type'
            else null 
        end as primary_link_type,
        case 
            when jsonb_array_length(data->'link') > 0 
            then data->'link'->0->>'link'
            else null 
        end as primary_link_url,
        
        -- Extract dietary restrictions array
        jsonb_array_length(data->'patronProfileDietaryRestriction') as dietary_restriction_count,
        data->'patronProfileDietaryRestriction' as dietary_restrictions,
        
        -- Extract hospitality preferences array
        jsonb_array_length(data->'patronProfileHospitalityPreference') as hospitality_preference_count,
        data->'patronProfileHospitalityPreference' as hospitality_preferences,
        
        -- Extract aversions
        data->>'patronProfileAversions' as aversions,
        
        -- Business and metadata
        (data->>'businessGroupId')::int as business_group_id,
        data->>'optInSource' as opt_in_source,
        (data->>'isArchived')::boolean as is_archived,
        (data->>'canEdit')::boolean as can_edit,
        (data->>'isTockVerified')::boolean as is_tock_verified,
        
        -- Timestamps
        to_timestamp((data->>'createdAtTimestamp')::bigint / 1000) as created_at,
        to_timestamp((data->>'updatedAtTimestamp')::bigint / 1000) as updated_at,
        
        -- Extract attribute array (for external system IDs like Commerce7)
        jsonb_array_length(data->'attribute') as attribute_count,
        data->'attribute' as attributes,
        
        -- Extract business guest profile array
        jsonb_array_length(data->'businessGuestProfile') as business_guest_profile_count,
        data->'businessGuestProfile' as business_guest_profiles,
        
        -- Extract tag array
        jsonb_array_length(data->'tag') as tag_count,
        data->'tag' as tags,
        
        -- Airbyte metadata
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_tock_guest_hashid,
        last_processed_at
        
    from source_data
)

select * from parsed_data
