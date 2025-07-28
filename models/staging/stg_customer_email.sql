{{ config(materialized='incremental',
          unique_key='email_id',
          incremental_strategy='merge') }}

with base as (
    select customer_id, _customer_json as c, updated_at
    from {{ ref('stg_customer') }}
),

emails as (
    select
        customer_id,
        (e->>'id')::uuid       as email_id,
        e->>'email'            as email,
        e->>'status'           as status,
        updated_at
    from base
    cross join lateral jsonb_array_elements(c->'emails') e
)

select * from emails

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at) - interval '3 days','2000‑01‑01') from {{ this }})
{% endif %}
