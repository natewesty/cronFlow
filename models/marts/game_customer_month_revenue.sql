-- Customer-month revenue (net: excluding tax, shipping, tip)
SELECT
  fo.customer_id,
  d.fiscal_year,
  d.fiscal_month as fiscal_month,  -- Using fiscal_month since fiscal_week doesn't exist
  sum(fo.subtotal)  -- Using subtotal from fct_order (already divided by 100)
  - sum(coalesce(fo.tax,0))
  - sum(coalesce(fo.shipping,0))
  - sum(coalesce(fo.tip,0)) as net_revenue
FROM {{ ref('fct_order') }} fo
JOIN {{ ref('dim_date') }} d ON fo.order_date_key = d.date_day
GROUP BY 1,2,3;
