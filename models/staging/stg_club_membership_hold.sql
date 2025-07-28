{{ config(
    materialized='incremental',
    unique_key='hold_id',
    incremental_strategy='merge'
) }}

with base as (
    select membership_id, _membership_json as m, updated_at
    from {{ ref('stg_club_membership') }}
),

holds as (
    select
        membership_id,
        (h->>'id')::uuid                 as hold_id,
        (h->>'startDate')::timestamptz   as hold_start_at,
        (h->>'endDate')::timestamptz     as hold_end_at,
        h->>'holdReason'                 as hold_reason,
        h->>'holdComments'               as hold_comments,
        updated_at
    from base
    cross join lateral jsonb_array_elements(m->'onHolds') h
)

select * from holds

{% if is_incremental() %}
  where updated_at >= (
        select coalesce(max(updated_at) - interval '3 days','2000-01-01')
        from {{ this }})
{% endif %}
