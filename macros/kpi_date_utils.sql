{% macro kpi_bounds_cte(as_of_alias='p') %}
-- Produces a CTE named window_bounds with aligned current/prior windows
-- Requires a CTE "params" providing {{ as_of_alias }}.as_of_date
window_bounds as (
  select
      {{ as_of_alias }}.as_of_date
    , dd.month_start
    , dd.quarter_start
    , dd.year_start
    , {{ as_of_alias }}.as_of_date - interval '27 days' as last28_start

    , (dd.month_start   - interval '1 year')::date   as prev_month_start
    , (dd.quarter_start - interval '1 year')::date   as prev_quarter_start
    , (dd.year_start    - interval '1 year')::date   as prev_year_start
    , ({{ as_of_alias }}.as_of_date - interval '27 days' - interval '1 year')::date as prev_last28_start
  from {{ ref('kpi_dim_date') }} dd
  join {{ as_of_alias }} on dd.date_key = {{ as_of_alias }}.as_of_date
)
{% endmacro %}

