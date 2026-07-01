{#-
  Central fiscal-year math.

  The fiscal year is 12 months long and begins on the 1st of a configurable
  start month (1-12). The start month is sourced, in priority order, from:
    1. public.dashboard_fiscal_config.fiscal_start_month  (written by the app)
    2. var('fiscal_start_month', 7)                        (fallback default)

  All other fiscal fields (year, name, month, quarter, boundaries) derive from
  the start month, so no model needs to hardcode July again.

  The expression macros accept an optional `s` (start month) so a model can
  resolve it once and pass it to every call, avoiding repeated config lookups.
-#}

{% macro fiscal_start_month() %}
  {%- set default_month = var('fiscal_start_month', 7) | int -%}
  {%- if execute -%}
    {%- set cfg_relation = var('raw_schema', 'public') ~ '.dashboard_fiscal_config' -%}
    {%- set exists_res = run_query("select to_regclass('" ~ cfg_relation ~ "') is not null as tbl_exists") -%}
    {%- if exists_res and exists_res.rows | length > 0 and exists_res.rows[0][0] -%}
      {%- set val_res = run_query('select fiscal_start_month from ' ~ cfg_relation ~ ' order by id limit 1') -%}
      {%- if val_res and val_res.rows | length > 0 and val_res.rows[0][0] is not none -%}
        {{ return(val_res.rows[0][0] | int) }}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(default_month) }}
{% endmacro %}


{#- FY start date (the 1st of the start month, on or before date_col). -#}
{% macro fiscal_year_start(date_col, s=none) %}
  {%- if s is none %}{% set s = fiscal_start_month() %}{% endif -%}
  {%- set off = s - 1 -%}
  (date_trunc('year', (({{ date_col }})::date - interval '{{ off }} months')) + interval '{{ off }} months')::date
{%- endmacro %}


{#- Last day of the fiscal year containing date_col. -#}
{% macro fiscal_year_end_boundary(date_col, s=none) %}
  {%- if s is none %}{% set s = fiscal_start_month() %}{% endif -%}
  (({{ fiscal_year_start(date_col, s) }}) + interval '1 year' - interval '1 day')::date
{%- endmacro %}


{#- FY designator, named by the calendar year in which the FY ends. -#}
{% macro fiscal_year(date_col, s=none) %}
  {%- if s is none %}{% set s = fiscal_start_month() %}{% endif -%}
  extract(year from ({{ fiscal_year_end_boundary(date_col, s) }}))::int
{%- endmacro %}


{#- 'FY' + end year, e.g. FY2026. -#}
{% macro fiscal_year_name(date_col, s=none) %}
  {%- if s is none %}{% set s = fiscal_start_month() %}{% endif -%}
  ('FY' || ({{ fiscal_year(date_col, s) }})::text)
{%- endmacro %}


{#- Position of date_col's month within the FY (start month => 1 ... => 12). -#}
{% macro fiscal_month(date_col, s=none) %}
  {%- if s is none %}{% set s = fiscal_start_month() %}{% endif -%}
  (((extract(month from ({{ date_col }})::date)::int - {{ s }} + 12) % 12) + 1)
{%- endmacro %}


{#- Fiscal quarter (1-4) that date_col falls in. -#}
{% macro fiscal_quarter(date_col, s=none) %}
  {%- if s is none %}{% set s = fiscal_start_month() %}{% endif -%}
  (floor((({{ fiscal_month(date_col, s) }}) - 1) / 3) + 1)::int
{%- endmacro %}
