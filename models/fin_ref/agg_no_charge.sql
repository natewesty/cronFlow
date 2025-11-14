{{ config(materialized='table') }}

with monthly_data as (
    select
        qb.month_end_date_fulfilled,
        qb.month_end_date_paid,
        qb.customer_id,
        qb.sku,
        qb.case_size,
        qb.quantity,
        qb.month_name,
        qb.product_subtotal,
        nca.no_charge_account,
        nca.no_charge_class
    from {{ ref('stg_qb_format_base') }} as qb
    inner join {{ ref('stg_no_charge_accounts') }} as nca
        on qb.customer_id = nca.customer_id
    where qb.in_month = true
),

aggregated as (
    select
        '50001' as account,
        month_end_date_fulfilled as transaction_date,
        sku as item,
        round(sum(quantity)::numeric / max(case_size)::numeric, 5) as quantity,
        no_charge_account as customer,
        no_charge_class as class_code,
        month_name
    from monthly_data
    where month_end_date_fulfilled = month_end_date_paid
      and product_subtotal = 0
    group by
        month_end_date_fulfilled,
        no_charge_account,
        no_charge_class,
        sku,
        month_name
    having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
),

-- Build one row per (month, customer) first, THEN apply the window function
month_customer_distinct as (
    select distinct
        transaction_date,
        customer
    from aggregated
),

numbered as (
    select
        transaction_date,
        customer,
        row_number() over (
            partition by transaction_date
            order by customer
        ) as month_ref_num
    from month_customer_distinct
)

select
    concat(a.month_name, 'InvAdj', n.month_ref_num) as ref_number,
    a.account,
    a.transaction_date,
    concat(a.month_name, ' No Charge Inv Adjust') as memo,
    a.item,
    a.quantity,
    a.customer,
    a.class_code
from aggregated a
join numbered n
  on n.transaction_date = a.transaction_date
 and n.customer   = a.customer
order by a.transaction_date desc, a.customer, a.item
