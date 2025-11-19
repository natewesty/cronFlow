{{ config(materialized='table') }}

with monthly_data as (
    select
        month_end_date_paid,
        sku,
        case_size,
        quantity,
        month_name,
        product_subtotal,
        class_code
    from {{ ref('stg_qb_format_base') }}
    where in_month = false
        and month_end_date_fulfilled is not null
        and is_refunded = false
),
export_tracking as (
    select
        payment_date,
        item,
        class_code,
        coalesce(sum(exported_quantity), 0) as total_exported_quantity
    from {{ ref('stg_deferred_revenue_export_tracking') }}
    group by payment_date, item, class_code
),
aggregated_base as (
    select
        month_end_date_paid as payment_date,
        sku as item,
        round(sum(quantity)::numeric / max(case_size)::numeric, 5) as current_quantity,
        class_code as class_code,
        month_name
    from monthly_data
    group by
        month_end_date_paid,
        sku,
        class_code,
        month_name
    having round(sum(quantity)::numeric / max(case_size)::numeric, 5) != 0
),
aggregated as (
    select
        '50001' as account,
        ab.payment_date,
        ab.item,
        -- Calculate remaining quantity: current_quantity - exported_quantity
        -- Only show records where remaining quantity > 0
        round(ab.current_quantity - coalesce(et.total_exported_quantity, 0), 5) as quantity,
        ab.class_code,
        ab.month_name
    from aggregated_base ab
    left join export_tracking et
        on et.payment_date = ab.payment_date
        and et.item = ab.item
        and et.class_code = ab.class_code
        and et.payment_date = ab.payment_date
    -- Only include records where remaining quantity > 0
    where round(ab.current_quantity - coalesce(et.total_exported_quantity, 0), 5) > 0
),
month_customer_distinct as (
    select distinct
        payment_date,
        class_code
    from aggregated
),
numbered as (
    select
        payment_date,
        class_code,
        row_number() over (
            partition by payment_date
            order by class_code
        ) as month_ref_num
    from month_customer_distinct
)
select
    concat(a.month_name, 'DefRev', n.month_ref_num) as ref_number,
    a.account,
    concat(a.month_name, ' Fulfilled Inv Adjust') as memo,
    a.item,
    a.quantity,
    case
        when a.class_code = '43 Inbound' then 'Inbound Unfulfilled'
        when a.class_code = '50 TR' then 'POS Unfulfilled'
        when a.class_code = '55 Events' then 'Event Unfulfilled'
        when a.class_code = '56 Ecommerce' then 'Web Unfulfilled'
        when a.class_code = '40 WH Sales' then 'Distribution Unfulfilled'
        when a.class_code = '80 Admin' then 'Admin Unfulfilled'
        when a.class_code = '54 Wine Club' then 'Wine Club Unfulfilled'
        when a.class_code = '30 Production' then 'Production Unfulfilled'
        when a.class_code = '60 Marketing' then 'Marketing Unfulfilled'
        when a.class_code = '88 Art/Shareholder' then 'Ownership Unfulfilled'
        else null
    end as customer,
    a.class_code,
    a.payment_date
from numbered n
inner join aggregated a
    on n.payment_date = a.payment_date
    and n.class_code = a.class_code
order by a.payment_date desc, a.class_code, a.item