{{ config(materialized='table') }}

with monthly_data as (
    select
        (date_trunc('month', fulfilled_date) + interval '1 month - 1 day')::date as month_end_date_fulfilled,
        (date_trunc('month', paid_date) + interval '1 month - 1 day')::date as month_end_date_paid,
        sku,
        extrapolated_price,
        case_size,
        unit_of_measure,
        quantity
    from {{ ref('stg_qb_format_base') }}
    where channel = 'Club'
        and (delivery_method = 'Pickup' or delivery_method = 'Carry Out')
        and case_size is not null
        and case_size > 0
)
select
    month_end_date_fulfilled,
    month_end_date_paid,
    sku,
    extrapolated_price,
    round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity_cases,
    unit_of_measure
from monthly_data
where month_end_date_fulfilled = month_end_date_paid
group by
    month_end_date_fulfilled,
    month_end_date_paid,
    sku,
    extrapolated_price,
    unit_of_measure
having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
order by month_end_date_fulfilled desc, sku

