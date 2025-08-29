-- Data Warehouse Views for Fantasy Revenue League
-- Create these views in your PostgreSQL data warehouse
-- Adjust table/column names to match your schema

-- Customer-week revenue (net: excluding tax, shipping, tip)
CREATE OR REPLACE VIEW game_customer_week_revenue AS
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
GROUP BY 1,2,3;

-- Visitors by week (unique customer-week visits)
CREATE OR REPLACE VIEW game_visitors_by_week AS
SELECT DISTINCT
  v.customer_id,
  d.fiscal_year,
  d.fiscal_week_of_year as fiscal_week
FROM fact_visit v
JOIN dim_fiscal_date d ON v.visit_date = d.calendar_date;

-- Helpful context: week start/end (for UI/context)
CREATE OR REPLACE VIEW game_fiscal_weeks AS
SELECT
  fiscal_year,
  fiscal_week_of_year as fiscal_week,
  min(calendar_date) as week_start,
  max(calendar_date) as week_end
FROM dim_fiscal_date
GROUP BY 1,2;
