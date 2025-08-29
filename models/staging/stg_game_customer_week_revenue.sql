-- Customer-week revenue (net: excluding tax, shipping, tip)
SELECT
  fo.customer_id,
  d.fiscal_year,
  d.fiscal_week_of_year as fiscal_week,
  sum(fo.sub_total_cents)/100.0
  - sum(coalesce(fo.tax_total_cents,0))/100.0
  - sum(coalesce(fo.ship_total_cents,0))/100.0
  - sum(coalesce(fo.tip_total_cents,0))/100.0 as net_revenue
FROM fact_order fo
JOIN dim_fiscal_date d ON date(fo.submitted_at) = d.calendar_date
GROUP BY 1,2,3
