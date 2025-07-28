{{ config(
    materialized='incremental',
    unique_key='hold_id',
    incremental_strategy='merge'
) }}

select
    hold_id,
    membership_id,
    date(hold_start_at)    as hold_start_date_key,
    date(hold_end_at)      as hold_end_date_key,
    hold_reason,
    hold_comments,
    hold_end_at - hold_start_at as hold_duration_days,
    updated_at
from {{ ref('stg_club_membership_hold') }}

{% if is_incremental() %}
 where updated_at >= (
        select coalesce(max(updated_at) - interval '3 days', date '2000-01-01')
        from {{ this }})
{% endif %}
