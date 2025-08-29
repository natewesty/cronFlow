-- Visitors by month (unique customer-month orders since fact_visit doesn't exist)
SELECT DISTINCT
  fo.customer_id,
  d.fiscal_year,
  d.fiscal_month as fiscal_month  -- Using fiscal_month since fiscal_week doesn't exist
FROM {{ ref('fct_order') }} fo
JOIN {{ ref('dim_date') }} d ON fo.order_date_key = d.date_day;
