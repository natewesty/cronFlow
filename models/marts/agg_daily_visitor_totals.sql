{{
  config(
    materialized='table'
  )
}}

with date_range as (
    -- Get the date range: from start of previous fiscal year to current date
    select 
        (select current_date_pacific from {{ ref('dim_date') }} limit 1) as current_date,
        (select current_date_pacific from {{ ref('dim_date') }} limit 1) - interval '2 years' as start_date
),

daily_reservations as (
    select
        date(ftr.reservation_datetime) as date_day,
        count(*) as total_reservations,
        sum(ftr.party_size) as total_visitors,
        avg(ftr.party_size) as avg_party_size,
        max(ftr.party_size) as max_party_size,
        min(ftr.party_size) as min_party_size
    from {{ ref('fct_tock_reservation') }} ftr
    cross join date_range dr
    where date(ftr.reservation_datetime) >= dr.start_date
    and date(ftr.reservation_datetime) <= dr.current_date
    group by date(ftr.reservation_datetime)
),

daily_reservations_by_attribution as (
    select
        date(ftr.reservation_datetime) as date_day,
        ftr.attribution,
        count(*) as reservations_by_attribution,
        sum(ftr.party_size) as visitors_by_attribution
    from {{ ref('fct_tock_reservation') }} ftr
    cross join date_range dr
    where date(ftr.reservation_datetime) >= dr.start_date
    and date(ftr.reservation_datetime) <= dr.current_date
    group by date(ftr.reservation_datetime), ftr.attribution
),

-- Get distinct attributions for conditional aggregation
distinct_attributions as (
    select distinct attribution
    from daily_reservations_by_attribution
    where attribution is not null
),

-- Create a complete date spine for the two-year period
date_spine as (
    select 
        dd.date_day,
        dd.fiscal_year,
        dd.fiscal_year_name,
        dd.fiscal_month,
        dd.fiscal_quarter,
        dd.month_name,
        dd.weekday_name,
        case 
            when dd.fiscal_year = (select fiscal_year from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1))
            then 'Current'
            when dd.fiscal_year = (select fiscal_year from {{ ref('dim_date') }} where date_day = (select current_date_pacific from {{ ref('dim_date') }} limit 1)) - 1
            then 'Previous'
            else 'Other'
        end as fiscal_year_period
    from {{ ref('dim_date') }} dd
    cross join date_range dr
    where dd.date_day >= dr.start_date
    and dd.date_day <= dr.current_date
)

select
    ds.date_day,
    ds.fiscal_year,
    ds.fiscal_year_name,
    ds.fiscal_month,
    ds.fiscal_quarter,
    ds.month_name,
    ds.weekday_name,
    ds.fiscal_year_period,
    
    -- Visitor metrics
    coalesce(dr.total_reservations, 0) as total_reservations,
    coalesce(dr.total_visitors, 0) as total_visitors,
    coalesce(dr.avg_party_size, 0) as avg_party_size,
    coalesce(dr.max_party_size, 0) as max_party_size,
    coalesce(dr.min_party_size, 0) as min_party_size,
    
    -- Visitor metrics by attribution (unattributed)
    coalesce(sum(case when dra.attribution is null then dra.visitors_by_attribution else 0 end), 0) as visitors_unattributed,
    coalesce(sum(case when dra.attribution is null then dra.reservations_by_attribution else 0 end), 0) as reservations_unattributed
    {%- set attributions = dbt_utils.get_column_values(
        table=ref('fct_tock_reservation'),
        column='attribution'
    ) -%}
    {%- if attributions -%}
    {%- for attribution in attributions -%}
    {%- if attribution is not none -%}
    ,
    coalesce(sum(case when dra.attribution = '{{ attribution }}' then dra.visitors_by_attribution else 0 end), 0) as visitors_{{ attribution | replace(' ', '_') | replace('-', '_') | replace("'", '') | lower }},
    coalesce(sum(case when dra.attribution = '{{ attribution }}' then dra.reservations_by_attribution else 0 end), 0) as reservations_{{ attribution | replace(' ', '_') | replace('-', '_') | replace("'", '') | lower }}
    {%- endif -%}
    {%- endfor -%}
    {% endif %}

from date_spine ds
left join daily_reservations dr on ds.date_day = dr.date_day
left join daily_reservations_by_attribution dra on ds.date_day = dra.date_day
group by 
    ds.date_day,
    ds.fiscal_year,
    ds.fiscal_year_name,
    ds.fiscal_month,
    ds.fiscal_quarter,
    ds.month_name,
    ds.weekday_name,
    ds.fiscal_year_period,
    dr.total_reservations,
    dr.total_visitors,
    dr.avg_party_size,
    dr.max_party_size,
    dr.min_party_size
order by ds.date_day
