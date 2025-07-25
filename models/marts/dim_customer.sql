-- models/marts/dim_customer.sql
-- Customer dimension mart

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    is_active_club_member,
    club_title as current_club_title,
    updated_at,
    last_processed_at
from {{ ref('stg_customer') }}
where data_type = 'current'
{% if is_incremental() %}
    and date(last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
{% endif %} 