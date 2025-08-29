-- Visitors by week (unique customer-week visits)
SELECT DISTINCT
  v.customer_id,
  d.fiscal_year,
  d.fiscal_week_of_year as fiscal_week
FROM fact_visit v
JOIN dim_fiscal_date d ON v.visit_date = d.calendar_date
