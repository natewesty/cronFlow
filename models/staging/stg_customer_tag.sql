{{ config(materialized='view') }}

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
        -- Convert UTC timestamps to Pacific Time
        (t->>'createdAt')::timestamptz AT TIME ZONE 'America/Los_Angeles' as created_at,
        (t->>'updatedAt')::timestamptz AT TIME ZONE 'America/Los_Angeles' as updated_at,
        t                               as _tag_json
    from base
    cross join lateral jsonb_array_elements(c->'tags') t
)

select * from tags

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at) - interval '3 days', date '2000-01-01') from {{ this }})
{% endif %}
