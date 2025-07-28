{{ config(
    materialized = 'incremental',
    unique_key   = 'customer_id||tag_id',
    incremental_strategy = 'merge'
) }}

with base as (
    select customer_id, _customer_json as c, updated_at
    from {{ ref('stg_customer') }}
),

tags as (
    select
        customer_id,
        (t->>'id')::uuid                as tag_id,
        t->>'title'                     as tag_title,
        t->>'objectType'                as object_type,
        t->>'type'                      as tag_type,
        t->>'appliesToCondition'        as applies_to_condition,
        (t->>'createdAt')::timestamptz  as created_at,
        (t->>'updatedAt')::timestamptz  as updated_at,
        t                               as _tag_json
    from base
    cross join lateral jsonb_array_elements(c->'tags') t
)

select * from tags

{% if is_incremental() %}
  where updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', '2000‑01‑01')
        from {{ this }}
  )
{% endif %}
