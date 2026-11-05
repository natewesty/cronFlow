-- Fails if any excluded order is still present in an order model.
-- Expected to fail between adding a seed row and the next pipeline run
-- (the post-hooks remove the rows on that run).
with excluded as (select order_id from {{ ref('stg_excluded_order') }})

select 'stg_order' as model, order_id from {{ ref('stg_order') }} where order_id in (select order_id from excluded)
union all
select 'stg_order_item', order_id from {{ ref('stg_order_item') }} where order_id in (select order_id from excluded)
union all
select 'fct_order', order_id from {{ ref('fct_order') }} where order_id in (select order_id from excluded)
union all
select 'fct_order_item', order_id from {{ ref('fct_order_item') }} where order_id in (select order_id from excluded)
