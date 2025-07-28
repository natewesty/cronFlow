{{ config(
    materialized = 'incremental',
    unique_key   = 'customer_id||group_id',
    incremental_strategy = 'merge'
) }}

with base as (
    select customer_id, _customer_json as c, updated_at
    from {{ ref('stg_customer') }}
),

groups as (
    select
        customer_id,
        (g->>'id')::uuid                as group_id,
        g->>'title'                     as group_title,
        g->>'objectType'                as object_type,
        g->>'type'                      as group_type,
        g->>'appliesToCondition'        as applies_to_condition,
        (g->>'createdAt')::timestamptz  as created_at,
        (g->>'updatedAt')::timestamptz  as updated_at,
        g                               as _group_json
    from base
    cross join lateral jsonb_array_elements(c->'groups') g
)

select * from groups

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at) - interval '3 days', date '2000-01-01') from {{ this }})
{% endif %}
