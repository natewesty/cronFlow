{{ config(
    materialized        = 'incremental',
    unique_key          = 'product_id',
    incremental_strategy= 'merge',
    on_schema_change    = 'sync_all_columns'
) }}

with src as (

    select
        (data->>'id')::uuid                as product_id,
        data->>'title'                     as title,
        data->>'subTitle'                  as sub_title,
        data->>'type'                      as product_type,
        data->>'slug'                      as slug,
        data->>'webStatus'                 as web_status,
        data->>'adminStatus'               as admin_status,
        (data->>'vendorId')::uuid          as vendor_id,
        (data->>'departmentId')::uuid      as department_id,
        data->'department'->>'title'       as department_title,

        -- Convert UTC timestamps to Pacific Time
        ((data->>'createdAt')::timestamptz AT TIME ZONE 'America/Los_Angeles') as created_at,
        ((data->>'updatedAt')::timestamptz AT TIME ZONE 'America/Los_Angeles') as updated_at,

        /* wine sub‑object */
        data->'wine'->>'type'              as wine_type,
        data->'wine'->>'varietal'          as varietal,
        (data->'wine'->>'vintage')::int    as vintage,
        (data->'wine'->>'alcoholPercentage')::numeric as abv,

        coalesce(last_processed_at, current_timestamp) as load_ts,
        data                               as _product_json

    from {{ source('raw','raw_product') }}

    {% if is_incremental() %}
      where last_processed_at >
            (select coalesce(max(load_ts), date '2000-01-01') from {{ this }})
    {% endif %}
),

dedup as (
    select *
         , row_number() over (
               partition by product_id
               order by updated_at desc, load_ts desc
           ) as rn
    from src
)

select * from dedup
where rn = 1
