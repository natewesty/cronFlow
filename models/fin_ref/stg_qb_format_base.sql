{{ config(materialized='view') }}

with base as (
    select
        customer_id,
        sku,
        paid_date,
        fulfilled_date,
        (date_trunc('month', fulfilled_date) + interval '1 month - 1 day')::date as month_end_date_fulfilled,
        (date_trunc('month', paid_date) + interval '1 month - 1 day')::date as month_end_date_paid,
        extract(month from paid_date) as month_number,
        case
            when extract(month from paid_date) = 1 then 'Jan'
            when extract(month from paid_date) = 2 then 'Feb'
            when extract(month from paid_date) = 3 then 'Mar'
            when extract(month from paid_date) = 4 then 'Apr'
            when extract(month from paid_date) = 5 then 'May'
            when extract(month from paid_date) = 6 then 'Jun'
            when extract(month from paid_date) = 7 then 'Jul'
            when extract(month from paid_date) = 8 then 'Aug'
            when extract(month from paid_date) = 9 then 'Sept'
            when extract(month from paid_date) = 10 then 'Oct'
            when extract(month from paid_date) = 11 then 'Nov'
            when extract(month from paid_date) = 12 then 'Dec'
            else null
        end as month_name,
        channel,
        delivery_method,
        quantity,
        product_subtotal,
        extrapolated_price,
        case_size,
        unit_of_measure,
        event_fee_or_wine,
        state_code,
        (date_trunc('month', fulfilled_date) + interval '1 month - 1 day')::date = 
        (date_trunc('month', paid_date) + interval '1 month - 1 day')::date as in_month
    from {{ ref('fct_order_item') }}
    where external_order_vendor IS NULL
    and item_type in ('Bundle', 'General Merchandise', 'Wine')
)
select 
    customer_id,
    sku,
    paid_date,
    fulfilled_date,
    month_end_date_fulfilled,
    month_end_date_paid,
    month_number,
    month_name,
    channel,
    delivery_method,
    quantity,
    product_subtotal,
    extrapolated_price,
    case_size,
    unit_of_measure,
    in_month,
    event_fee_or_wine,
    state_code,
    case
        when channel = 'Club' then '54 Wine Club'
        when channel = 'Inbound' then '43 Inbound'
        when channel = 'Web' then '56 Ecommerce'
        when channel = 'POS' and event_fee_or_wine is null then '50 TR'
        when channel = 'POS' and event_fee_or_wine is not null then '55 Events'
    end as class_code,
    case
        when channel = 'Club' and delivery_method IN ('Pickup', 'Carry Out') and in_month then concat(month_name,'C7.1')
        when channel = 'Club' and fulfilled_date is null then concat(month_name,'C7.4')
        when channel = 'Club' and delivery_method = 'Ship' and state_code = 'CA' and in_month then concat(month_name,'C7.2')
        when channel = 'Club' and delivery_method = 'Ship' and state_code != 'CA' and in_month then concat(month_name,'C7.3')
        when channel = 'Inbound' and delivery_method IN ('Pickup', 'Carry Out') and in_month then concat(month_name,'C7.5')
        when channel = 'Inbound' and fulfilled_date is null then concat(month_name,'C7.8')
        when channel = 'Inbound' and delivery_method = 'Ship' and state_code = 'CA' and in_month then concat(month_name,'C7.6')
        when channel = 'Inbound' and delivery_method = 'Ship' and state_code != 'CA' and in_month then concat(month_name,'C7.7')
        when channel = 'Web' and delivery_method IN ('Pickup', 'Carry Out') and in_month then concat(month_name,'C7.9')
        when channel = 'Web' and fulfilled_date is null then concat(month_name,'C7.12')
        when channel = 'Web' and delivery_method = 'Ship' and state_code = 'CA' and in_month then concat(month_name,'C7.10')
        when channel = 'Web' and delivery_method = 'Ship' and state_code != 'CA' and in_month then concat(month_name,'C7.11')
        when channel = 'POS' and delivery_method IN ('Pickup', 'Carry Out') and event_fee_or_wine is null and in_month then concat(month_name,'C7.13')
        when channel = 'POS' and fulfilled_date is null and event_fee_or_wine is null then concat(month_name,'C7.16')
        when channel = 'POS' and delivery_method = 'Ship' and state_code = 'CA' and event_fee_or_wine is null and in_month then concat(month_name,'C7.14')
        when channel = 'POS' and delivery_method = 'Ship' and state_code != 'CA' and event_fee_or_wine is null and in_month then concat(month_name,'C7.15')
        when channel = 'POS' and event_fee_or_wine = 'Event Fee' and in_month then concat(month_name,'C7.17')
        when channel = 'POS' and event_fee_or_wine = 'Event Wine' and in_month then concat(month_name,'C7.18')
        else null
    end as ref_number
from base
