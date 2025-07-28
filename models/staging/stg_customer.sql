{{ config(materialized='incremental',
          unique_key='customer_id',
          incremental_strategy='merge',
          on_schema_change='sync_all_columns') }}

with src as (
    select
        (c->>'id')::uuid                     as customer_id,
        c->>'honorific'                      as honorific,
        c->>'firstName'                      as first_name,
        c->>'lastName'                       as last_name,
        (c->>'birthDate')::date              as birth_date,
        c->>'city'                           as city,
        c->>'stateCode'                      as state_code,
        c->>'zipCode'                        as postal_code,
        c->>'countryCode'                    as country_code,
        c->>'emailMarketingStatus'           as email_mkt_status,
        (c->>'lastActivityDate')::timestamptz  as last_activity_at,
        (c->>'createdAt')::timestamptz       as created_at,
        (c->>'updatedAt')::timestamptz       as updated_at,

        /* ────── orderInformation sub‑object ────── */
        (c->'orderInformation'->>'orderCount')::int          as order_count,
        (c->'orderInformation'->>'lifetimeValue')::bigint    as lifetime_value_cents,
        (c->'orderInformation'->>'grossProfit')::bigint      as gross_profit_cents,
        (c->'orderInformation'->>'acquisitionChannel')       as acquisition_channel,
        (c->'orderInformation'->>'currentClubTitle')         as current_club_title,
        (c->'orderInformation'->>'isActiveClubMember')::bool as is_active_club_member,
        (c->'orderInformation'->>'lastOrderId')::uuid        as last_order_id,
        (c->'orderInformation'->>'lastOrderDate')::timestamptz as last_order_at,

        /* flags */
        (c->>'hasAccount')::bool              as has_account,
        r.load_ts,
        c                                     as _customer_json
    from {{ source('raw','raw_customer') }} r
    cross join lateral jsonb_array_elements(r.payload->'customers') as c
),

dedup as (
  select *, row_number() over (partition by customer_id order by updated_at desc, load_ts desc) rn
  from src
)

select * from dedup
where rn = 1

{% if is_incremental() %}
  and updated_at >= (select coalesce(max(updated_at) - interval '3 days','2000‑01‑01') from {{ this }})
{% endif %}
