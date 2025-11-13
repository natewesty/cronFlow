with base as (
    select distinct on (customer_id)
        customer_id,
        first_name,
        last_name,
        no_charge_guest_type
    from {{ ref('dim_customer')}}
    where no_charge_guest_type is not null
)
select
    customer_id,
    trim(concat(first_name, ' ', last_name)) as no_charge_account,
    case
        when trim(concat(first_name, ' ', last_name)) = 'Admin Damaged' then '80 Admin'
        when trim(concat(first_name, ' ', last_name)) = 'Admin Donations' then '80 Admin'
        when trim(concat(first_name, ' ', last_name)) = 'Admin Employee Comp' then '80 Admin'
        when trim(concat(first_name, ' ', last_name)) = 'Admin Samples' then '80 Admin'
        when trim(concat(first_name, ' ', last_name)) = 'Admin Secondary Samples' then '80 Admin'
        when trim(concat(first_name, ' ', last_name)) = 'Culinary Samples' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Culinary Secondary Samples' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Distribution Damaged' then '40 WH Sales'
        when trim(concat(first_name, ' ', last_name)) = 'Distribution Donations' then '40 WH Sales'
        when trim(concat(first_name, ' ', last_name)) = 'Distribution Employee Comp' then '40 WH Sales'
        when trim(concat(first_name, ' ', last_name)) = 'Distribution Samples' then '40 WH Sales'
        when trim(concat(first_name, ' ', last_name)) = 'Distribution Secondary Samples' then '40 WH Sales'
        when trim(concat(first_name, ' ', last_name)) = 'Event Damaged' then '55 Events'
        when trim(concat(first_name, ' ', last_name)) = 'Event Donations' then '55 Events'
        when trim(concat(first_name, ' ', last_name)) = 'Event Employee Comp' then '55 Events'
        when trim(concat(first_name, ' ', last_name)) = 'Event Pours' then '55 Events'
        when trim(concat(first_name, ' ', last_name)) = 'Event Samples' then '55 Events'
        when trim(concat(first_name, ' ', last_name)) = 'Event Secondary Samples' then '55 Events'
        when trim(concat(first_name, ' ', last_name)) = 'Export Samples' then '48 Export'
        when trim(concat(first_name, ' ', last_name)) = 'Inbound Pours' then '43 Inbound'
        when trim(concat(first_name, ' ', last_name)) = 'Marketing Damaged' then '60 Marketing'
        when trim(concat(first_name, ' ', last_name)) = 'Marketing Donations' then '60 Marketing'
        when trim(concat(first_name, ' ', last_name)) = 'Marketing Employee Comp' then '60 Marketing'
        when trim(concat(first_name, ' ', last_name)) = 'Marketing Samples' then '60 Marketing'
        when trim(concat(first_name, ' ', last_name)) = 'Marketing Secondary Samples' then '60 Marketing'
        when trim(concat(first_name, ' ', last_name)) = 'Ownership Damaged' then '88 Art/Shareholder'
        when trim(concat(first_name, ' ', last_name)) = 'Ownership Donations' then '88 Art/Shareholder'
        when trim(concat(first_name, ' ', last_name)) = 'Ownership Employee Comp' then '88 Art/Shareholder'
        when trim(concat(first_name, ' ', last_name)) = 'Ownership Samples' then '88 Art/Shareholder'
        when trim(concat(first_name, ' ', last_name)) = 'Ownership Secondary Samples' then '88 Art/Shareholder'
        when trim(concat(first_name, ' ', last_name)) = 'Production Damaged' then '30 Production'
        when trim(concat(first_name, ' ', last_name)) = 'Production Donations' then '30 Production'
        when trim(concat(first_name, ' ', last_name)) = 'Production Employee Comp' then '30 Production'
        when trim(concat(first_name, ' ', last_name)) = 'Production Samples' then '30 Production'
        when trim(concat(first_name, ' ', last_name)) = 'Production Secondary Samples' then '30 Production'
        when trim(concat(first_name, ' ', last_name)) = 'Tasting Room Damaged' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Tasting Room Donations' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Tasting Room Employee Comp' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Tasting Room Pours' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Tasting Room Samples' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Tasting Room Secondary Samples' then '50 TR'
        when trim(concat(first_name, ' ', last_name)) = 'Wine Club Damaged' then '54 Wine Club'
        when trim(concat(first_name, ' ', last_name)) = 'Wine Club Donations' then '54 Wine Club'
        when trim(concat(first_name, ' ', last_name)) = 'Wine Club Employee Comp' then '54 Wine Club'
        when trim(concat(first_name, ' ', last_name)) = 'Wine Club Pours' then '54 Wine Club'
        when trim(concat(first_name, ' ', last_name)) = 'Wine Club Samples' then '54 Wine Club'
        when trim(concat(first_name, ' ', last_name)) = 'Wine Club Secondary Samples' then '54 Wine Club'
        else null
    end as no_charge_class
from base
order by trim(concat(first_name, ' ', last_name)) asc