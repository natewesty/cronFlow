-- Helpful context: month start/end (for UI/context) - using fiscal_month since fiscal_week doesn't exist
SELECT
  fiscal_year,
  fiscal_month as fiscal_month,
  min(date_day) as month_start,
  max(date_day) as month_end
FROM {{ ref('dim_date') }}
GROUP BY 1,2
