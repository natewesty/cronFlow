{{ config(
    materialized          = 'incremental',
    unique_key            = 'product_id',
    incremental_strategy  = 'merge',
    on_schema_change      = 'sync_all_columns'
) }}

with src as (
    select
        (p->>'id')::uuid                    as product_id,
        p->>'title'                         as title,
        p->>'subTitle'                      as sub_title,
        p->>'type'                          as product_type,
        p->>'slug'                          as slug,
        p->>'webStatus'                     as web_status,
        p->>'adminStatus'                   as admin_status,
        (p->>'vendorId')::uuid              as vendor_id,
        (p->>'departmentId')::uuid          as department_id,
        p->'department'->>'title'           as department_title,       -- nested object
        (p->>'createdAt')::timestamptz      as created_at,
        (p->>'updatedAt')::timestamptz      as updated_at,

        /* ── wine‑specific sub‑object (null for merch) ── */
        p->'wine'->>'type'                  as wine_type,
        p->'wine'->>'varietal'              as varietal,
        (p->'wine'->>'vintage')::int        as vintage,
        (p->'wine'->>'alcoholPercentage')::numeric as abv,

        /* raw record */
        r.load_ts,
        p                                   as _product_json
    from {{ source('raw','product_ingest') }} r
    cross join lateral jsonb_array_elements(r.payload->'products') p
),

dedup as (
  select *, row_number() over (partition by product_id order by updated_at desc, load_ts desc) rn
  from src
)

select * from dedup where rn = 1

{% if is_incremental() %}
  and updated_at >= (select coalesce(max(updated_at) - interval '3 days','2000‑01‑01') from {{ this }})
{% endif %}
