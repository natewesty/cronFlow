{{ config(materialized='table') }}

with monthly_data as (
    select
        month_end_date_paid,
        sku,
        extrapolated_price,
        product_subtotal,
        case_size,
        unit_of_measure,
        ref_number,
        class_code,
        quantity,
        month_name
    from {{ ref('stg_qb_format_base') }}
    where fulfilled_date is null
        and quantity > 0
        and product_subtotal != 0
)
select
    month_end_date_paid as transaction_date,
    concat(extract(year from month_end_date_paid), ' - ', month_name) as display_time,
    max(class_code) as class_code,
    sku as item,
    round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity,
    unit_of_measure,
    (round(sum(quantity)::numeric / max(case_size)::numeric, 5) * max(extrapolated_price))::money as value_on_street
from monthly_data
group by
    month_end_date_paid,
    month_name,
    sku,
    unit_of_measure
having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
order by month_end_date_paid desc, month_name, class_code, sku

