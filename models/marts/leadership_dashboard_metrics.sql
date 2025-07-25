-- models/marts/leadership_dashboard_metrics.sql
-- Leadership dashboard metrics mart

{{ config(
    materialized='incremental',
    unique_key='metric_date',
    incremental_strategy='merge'
) }}

with current_fiscal_year as (
    select 
        fiscal_year,
        fiscal_year_label,
        fiscal_year_start_date,
        fiscal_year_end_date
    from {{ ref('dim_fiscal_date') }}
    where calendar_date = current_date
    limit 1
),

event_orders as (
    select distinct order_number
    from {{ ref('stg_order_event_c7') }}
    where order_number is not null
),

tr_wine_sales as (
    select
        current_date as metric_date,
        'tr_wine_sales' as metric_name,
        'Total wine sales from POS orders in current fiscal year (excluding events)' as metric_description,
        sum(so.subtotal) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(so.last_processed_at) as last_processed_at
    from {{ ref('stg_order') }} so
    cross join current_fiscal_year cfy
    left join event_orders eo on so.order_number = eo.order_number
    where so.channel = 'POS'
        and date(so.order_paid_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and eo.order_number is null
        and so.order_paid_date is not null
    {% if is_incremental() %}
        and date(so.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
),

club_sales as (
    select
        current_date as metric_date,
        'club_sales' as metric_name,
        'Total sales from Club channel orders in current fiscal year' as metric_description,
        sum(so.subtotal) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(so.last_processed_at) as last_processed_at
    from {{ ref('stg_order') }} so
    cross join current_fiscal_year cfy
    where so.channel = 'Club'
        and date(so.order_paid_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and so.order_paid_date is not null
    {% if is_incremental() %}
        and date(so.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
),

ecomm_sales as (
    select
        current_date as metric_date,
        'ecomm_sales' as metric_name,
        'Total sales from Web channel orders in current fiscal year' as metric_description,
        sum(so.subtotal) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(so.last_processed_at) as last_processed_at
    from {{ ref('stg_order') }} so
    cross join current_fiscal_year cfy
    where so.channel = 'Web'
        and date(so.order_paid_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and so.order_paid_date is not null
    {% if is_incremental() %}
        and date(so.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
),

phone_sales as (
    select
        current_date as metric_date,
        'phone_sales' as metric_name,
        'Total sales from Inbound channel orders in current fiscal year' as metric_description,
        sum(so.subtotal) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(so.last_processed_at) as last_processed_at
    from {{ ref('stg_order') }} so
    cross join current_fiscal_year cfy
    where so.channel = 'Inbound'
        and date(so.order_paid_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and so.order_paid_date is not null
    {% if is_incremental() %}
        and date(so.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
),

event_fees as (
    select
        current_date as metric_date,
        'event_fees' as metric_name,
        'Total event fees realized in current fiscal year' as metric_description,
        sum(soe.subtotal) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(soe.last_processed_at) as last_processed_at
    from {{ ref('stg_order_event_c7') }} soe
    cross join current_fiscal_year cfy
    where soe.event_fee_or_wine = 'Event Fee'
        and date(soe.updated_at) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and date(soe.event_revenue_realization_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and soe.updated_at is not null
        and soe.event_revenue_realization_date is not null
    {% if is_incremental() %}
        and date(soe.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
),

event_wine as (
    select
        current_date as metric_date,
        'event_wine' as metric_name,
        'Total event wine sales realized in current fiscal year' as metric_description,
        sum(soe.subtotal) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(soe.last_processed_at) as last_processed_at
    from {{ ref('stg_order_event_c7') }} soe
    cross join current_fiscal_year cfy
    where soe.event_fee_or_wine = 'Event Wine'
        and date(soe.updated_at) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and date(soe.event_revenue_realization_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and soe.updated_at is not null
        and soe.event_revenue_realization_date is not null
    {% if is_incremental() %}
        and date(soe.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
),

shipping as (
    select
        current_date as metric_date,
        'shipping' as metric_name,
        'Total shipping revenue in current fiscal year' as metric_description,
        sum(so.ship_total) as metric_value,
        cfy.fiscal_year,
        cfy.fiscal_year_label,
        max(so.last_processed_at) as last_processed_at
    from {{ ref('stg_order') }} so
    cross join current_fiscal_year cfy
    where date(so.order_paid_date) between cfy.fiscal_year_start_date and cfy.fiscal_year_end_date
        and so.order_paid_date is not null
    {% if is_incremental() %}
        and date(so.last_processed_at) > (select max(date(last_processed_at)) from {{ this }})
    {% endif %}
    group by 
        cfy.fiscal_year,
        cfy.fiscal_year_label
)

select * from tr_wine_sales
union all
select * from club_sales
union all
select * from ecomm_sales
union all
select * from phone_sales
union all
select * from event_fees
union all
select * from event_wine
union all
select * from shipping 