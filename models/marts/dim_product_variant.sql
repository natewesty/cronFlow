{{ config(materialized='table') }}

select
    pv.variant_id                               as product_variant_id,
    pv.product_id,
    p.title                                     as product_title,
    pv.variant_title,
    coalesce(p.wine_type, p.product_type)       as product_type,
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
    p.updated_at
from {{ ref('stg_product_variant') }} pv
join {{ ref('stg_product') }} p
  on p.product_id = pv.product_id
