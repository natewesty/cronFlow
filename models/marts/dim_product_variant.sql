{{ config(materialized='table') }}

with base as (
    select
        pv.variant_id                               as product_variant_id,
        pv.product_id,
        p.title                                     as product_title,
        pv.sub_title,
        pv.variant_title,
        p.product_type                              as product_type,
        p.varietal,
        p.vintage,
        pv.sku,
        pv.price_cents   / 100.0                    as price,
        pv.cogs_cents    / 100.0                    as cost_of_good,
        pv.volume_ml,
        pv.abv,
        pv.has_inventory,
        pv.has_shipping,
        p.department_title,
        p.web_status,
        p.admin_status,
        p.created_at,
        p.updated_at,
        case
            when p.product_type = 'Wine' and pv.volume_ml = 750 then 12
            when p.product_type = 'Wine' and pv.volume_ml = 1500 then 6
            else 1
        end                                         as case_size
    from {{ ref('stg_product_variant') }} pv
    join {{ ref('stg_product') }} p
      on p.product_id = pv.product_id
)
select
    product_variant_id,
    product_id,
    product_title,
    sub_title,
    variant_title,
    product_type,
    varietal,
    vintage,
    sku,
    price,
    cost_of_good,
    volume_ml,
    abv,
    case_size,
    case
        when case_size = 1 then 'EA'
        else 'CS'
    end                                             as unit_of_measure,
    case_size * price    as extrapolated_price,
    has_inventory,
    has_shipping,
    department_title,
    web_status,
    admin_status,
    created_at,
    updated_at

from base
