{{ config(materialized='table') }}

with monthly_data as (
    select
        qb.month_end_date_fulfilled,
        qb.month_end_date_paid,
        qb.customer_id,
        qb.sku,
        qb.product_subtotal,
        qb.case_size,
        qb.unit_of_measure,
        qb.ref_number,
        qb.quantity,
        nca.no_charge_account,
        nca.no_charge_class
    from {{ ref('stg_qb_format_base') }} as qb
    inner join {{ ref('stg_no_charge_accounts') }} as nca
        on qb.customer_id = nca.customer_id
)
select
    no_charge_account as customer,
    month_end_date_fulfilled as transaction_date,
    ref_number,
    no_charge_class as class_code,
    sku as item,
    0::money as price,
    round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity,
    unit_of_measure,
    '5001' as deposit_to
from monthly_data
where ref_number is not null
    and month_end_date_fulfilled = month_end_date_paid
group by
    month_end_date_fulfilled,
    no_charge_account,
    ref_number,
    no_charge_class,
    sku,
    unit_of_measure
having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
order by month_end_date_fulfilled desc, no_charge_account, sku