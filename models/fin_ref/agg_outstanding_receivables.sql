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
    max(class_code) as class_code,
    sku as item,
    round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity,
    unit_of_measure
from monthly_data
group by
    month_end_date_paid,
    sku,
    unit_of_measure
having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
order by month_end_date_paid desc, class_code, sku

