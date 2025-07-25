-- models/staging/stg_order_club.sql
-- Staging model for Commerce7 order club packages

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        data->>'id' as order_id,
        data->'club'->>'clubId' as club_id,
        data->'club'->>'clubTitle' as club_title,
        data->'club'->>'clubPackageId' as club_package_id,
        data->'club'->>'clubPackageTitle' as club_package_title,
        data->'club'->>'shipmentBuildStatus' as shipment_build_status,
        data->>'updatedAt' as updated_at,
        last_processed_at
    from {{ source('raw', 'raw_order') }}
    where jsonb_typeof(data->'club') = 'object'
    {% if is_incremental() %}
        and date_trunc('day', last_processed_at) > (select max(date_trunc('day', last_processed_at)) from {{ this }})
    {% endif %}
)

select * from base 