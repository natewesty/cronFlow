-- Helpful context: week start/end (for UI/context)
SELECT
  fiscal_year,
  fiscal_week_of_year as fiscal_week,
  min(calendar_date) as week_start,
  max(calendar_date) as week_end
FROM dim_fiscal_date
GROUP BY 1,2
