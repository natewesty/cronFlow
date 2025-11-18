{{ config(materialized='table') }}

with monthly_data as (
    select
        month_end_date_fulfilled,
        month_end_date_paid,
        sku,
        product_subtotal,
        case_size,
        unit_of_measure,
        ref_number,
        class_code,
        quantity,
        unit_price_from_order,
        (unit_price_from_order * case_size) as extrapolated_price_discounted
    from {{ ref('stg_qb_format_base') }}
    where class_code = '54 Wine Club'
        and ref_number like '%.4'
        and case_size is not null
        and case_size > 0
        and product_subtotal != 0
        and is_full_price = false
)
select
    'Club Unfulfilled' as customer,
    month_end_date_paid as transaction_date,
    max(ref_number) as ref_number,
    max(class_code) as class_code,
    sku as item,
    extrapolated_price_discounted::money as price,
    round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity,
    unit_of_measure,
    unit_price_from_order,
    extrapolated_price_discounted::money as extrapolated_price_discounted,
    '11300' as deposit_to
from monthly_data
group by
    month_end_date_paid,
    sku,
    unit_price_from_order,
    extrapolated_price_discounted,
    unit_of_measure
having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
order by month_end_date_paid desc, sku

