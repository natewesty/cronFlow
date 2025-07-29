{{ config(
    materialized        = 'incremental',
    unique_key          = 'customer_id',
    incremental_strategy= 'merge',
    on_schema_change    = 'sync_all_columns'
) }}

with src as (

    select
        (data->>'id')::uuid                     as customer_id,
        data->>'honorific'                      as honorific,
        data->>'firstName'                      as first_name,
        data->>'lastName'                       as last_name,
        (data->>'birthDate')::date              as birth_date,
        data->>'city'                           as city,
        data->>'stateCode'                      as state_code,
        data->>'zipCode'                        as postal_code,
        data->>'countryCode'                    as country_code,
        data->>'emailMarketingStatus'           as email_mkt_status,
        (data->>'lastActivityDate')::timestamptz  as last_activity_at,
        (data->>'createdAt')::timestamptz       as created_at,
        (data->>'updatedAt')::timestamptz       as updated_at,

        /* orderInformation sub‑object */
        (data->'orderInformation'->>'orderCount')::int          as order_count,
        (data->'orderInformation'->>'lifetimeValue')::bigint    as lifetime_value_cents,
        (data->'orderInformation'->>'grossProfit')::bigint      as gross_profit_cents,
        (data->'orderInformation'->>'acquisitionChannel')       as acquisition_channel,
        (data->'orderInformation'->>'currentClubTitle')         as current_club_title,
        (data->'orderInformation'->>'isActiveClubMember')::bool as is_active_club_member,
        (data->'orderInformation'->>'lastOrderId')::uuid        as last_order_id,
        (data->'orderInformation'->>'lastOrderDate')::timestamptz as last_order_at,

        /* flags */
        (data->>'hasAccount')::bool              as has_account,

        /* bookkeeping */
        coalesce(last_processed_at, current_timestamp) as load_ts,
        data                                    as _customer_json

    from {{ source('raw','raw_customer') }}

    {% if is_incremental() %}
      where last_processed_at >
            (select coalesce(max(load_ts), date '2000-01-01') from {{ this }})
    {% endif %}
),

dedup as (
    select *
         , row_number() over (
               partition by customer_id
               order by updated_at desc, load_ts desc
           ) as rn
    from src
)

select * from dedup
where rn = 1
