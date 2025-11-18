{{ config(materialized='table') }}

with monthly_data as (
    select
        month_end_date_fulfilled,
        month_end_date_paid,
        sku,
        extrapolated_price,
        product_subtotal,
        case_size,
        unit_of_measure,
        ref_number,
        class_code,
        quantity
    from {{ ref('stg_qb_format_base') }}
    where class_code = '55 Events'
        and ref_number like '%.18'
        and case_size is not null
        and case_size > 0
        and in_month = true
        and product_subtotal != 0
        and is_full_price = true
)
select
    'Event Wine' as customer,
    month_end_date_fulfilled as transaction_date,
    max(ref_number) as ref_number,
    max(class_code) as class_code,
    sku as item,
    extrapolated_price::money as price,
    round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity,
    unit_of_measure,
    '11300' as deposit_to
from monthly_data
where month_end_date_fulfilled = month_end_date_paid
group by
    month_end_date_fulfilled,
    sku,
    extrapolated_price,
    unit_of_measure
having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
order by month_end_date_fulfilled desc, sku