-- models/marts/agg_vintage_depletion_summary.sql
{{
  config(
    materialized='table',
    description='Aggregated vintage depletion metrics for wine tracking dashboard',
    indexes=[
      {'columns': ['sku'], 'type': 'btree'},
      {'columns': ['product_id'], 'type': 'btree'},
      {'columns': ['vintage'], 'type': 'btree'},
      {'columns': ['web_status', 'admin_status'], 'type': 'btree'}
    ]
  )
}}

with wine_vintages as (
    -- Filter for wine products only - using EXACT column names from your existing models
    select
        dpv.product_variant_id,
        dpv.product_id,
        dpv.sku,
        dpv.variant_title,
        dp.title as product_title,
        dp.varietal,
        dp.vintage,
        dp.wine_type,
        dp.department_title,
        dp.web_status,
        dp.admin_status,
        dpv.has_inventory,
        dpv.price,
        dpv.cost_of_good,
        dpv.volume_ml,
        dpv.abv,
        dp.created_at as vintage_created_at,
        dp.updated_at as vintage_updated_at,
        
        -- Determine if this is an active wine vintage
        case 
            when dp.web_status = 'Available' and dp.admin_status = 'Available' 
            then 'active'
            when dp.web_status = 'Draft' or dp.admin_status = 'Draft'
            then 'planning'
            else 'previous'
        end as vintage_status
        
    from {{ ref('dim_product_variant') }} dpv
    join {{ ref('stg_product') }} dp on dpv.product_id = dp.product_id
    where dp.wine_type is not null  -- Only wine products
      and dp.vintage is not null    -- Must have vintage year
      and dpv.sku is not null       -- Must have SKU
),

sales_metrics as (
    select
        foi.sku,
        
        -- All-time sales (excluding Club) - using EXACT column names from fct_order_item
        sum(case when foi.channel != 'Club' then foi.quantity else 0 end) as cumulative_bottles_sold,
        
        -- Club sales (all-time)
        sum(case when foi.channel = 'Club' then foi.quantity else 0 end) as club_bottles_sold,
        
        -- Recent sales (last 30 days, excluding Club)
        sum(case 
            when foi.channel != 'Club' 
            and foi.paid_at >= current_date - interval '30 days' 
            then foi.quantity 
            else 0 
        end) as recent_bottles_sold_30d,
        
        -- Days with sales (last 30 days)
        count(distinct case 
            when foi.channel != 'Club' 
            and foi.paid_at >= current_date - interval '30 days' 
            then date(foi.paid_at) 
        end) as days_with_sales_30d,
        
        -- First sale date (excluding Club)
        min(case when foi.channel != 'Club' then foi.paid_at end) as first_sale_date,
        
        -- Last sale date (excluding Club)
        max(case when foi.channel != 'Club' then foi.paid_at end) as last_sale_date,
        
        -- Total order count (excluding Club)
        count(distinct case when foi.channel != 'Club' then foi.order_id end) as total_orders,
        
        -- Total order count (Club only)
        count(distinct case when foi.channel = 'Club' then foi.order_id end) as club_orders
        
    from {{ ref('fct_order_item') }} foi
    where foi.item_type = 'Wine'  -- Only wine items
    group by foi.sku
),

depletion_calculations as (
    select
        wv.*,
        coalesce(sm.cumulative_bottles_sold, 0) as cumulative_bottles_sold,
        coalesce(sm.club_bottles_sold, 0) as club_bottles_sold,
        coalesce(sm.recent_bottles_sold_30d, 0) as recent_bottles_sold_30d,
        coalesce(sm.days_with_sales_30d, 0) as days_with_sales_30d,
        sm.first_sale_date,
        sm.last_sale_date,
        coalesce(sm.total_orders, 0) as total_orders,
        coalesce(sm.club_orders, 0) as club_orders,
        
        -- Effective release date (first sale or vintage creation)
        coalesce(sm.first_sale_date, wv.vintage_created_at) as effective_release_date,
        
        -- Days since release
        extract(day from (current_date - coalesce(sm.first_sale_date, wv.vintage_created_at))) as days_since_release,
        
        -- Daily depletion rate (based on sales days only)
        case 
            when sm.days_with_sales_30d > 0 
            then round(sm.recent_bottles_sold_30d::numeric / sm.days_with_sales_30d, 2)
            else 0 
        end as daily_depletion_rate,
        
        -- Daily depletion rate over 30-day period
        case 
            when sm.recent_bottles_sold_30d > 0 
            then round(sm.recent_bottles_sold_30d::numeric / 30, 2)
            else 0 
        end as daily_depletion_rate_over_period
        
    from wine_vintages wv
    left join sales_metrics sm on wv.sku = sm.sku
)

select
    product_variant_id,
    product_id,
    sku,
    product_title,
    variant_title,
    varietal,
    vintage,
    wine_type,
    department_title,
    vintage_status,
    web_status,
    admin_status,
    has_inventory,
    price,
    cost_of_good,
    volume_ml,
    abv,
    
    -- Sales metrics
    cumulative_bottles_sold,
    club_bottles_sold,
    recent_bottles_sold_30d,
    days_with_sales_30d,
    total_orders,
    club_orders,
    
    -- Date metrics
    effective_release_date,
    greatest(days_since_release, 0) as days_since_release,
    first_sale_date,
    last_sale_date,
    vintage_created_at,
    vintage_updated_at,
    
    -- Calculated metrics
    daily_depletion_rate,
    daily_depletion_rate_over_period,
    
    -- Metadata
    current_date as calculation_date,
    'warehouse' as data_source
    
from depletion_calculations
where vintage_status in ('active', 'planning')  -- Focus on current vintages
order by varietal, vintage desc, product_title