with base as (
    select distinct on (customer_id)
        customer_id,
        first_name,
        last_name
    from {{ ref('dim_customer')}}
    where primary_email like '%@nocount.com'
)
select
    customer_id,
    concat(first_name, ' ', last_name) as no_charge_account,
    case
        when concat(first_name, ' ', last_name) = 'Admin Donations ' then '80 Admin'
        when concat(first_name, ' ', last_name) = 'Admin Employee Comp ' then '80 Admin'
        when concat(first_name, ' ', last_name) = 'Admin Samples ' then '80 Admin'
        when concat(first_name, ' ', last_name) = 'Admin Secondary Samples ' then '80 Admin'
        when concat(first_name, ' ', last_name) = 'Culinary Samples ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Culinary Secondary Samples ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Distribution Samples' then '40 WH Sales'
        when concat(first_name, ' ', last_name) = 'Event Pours ' then '55 Events'
        when concat(first_name, ' ', last_name) = 'Event Samples ' then '55 Events'
        when concat(first_name, ' ', last_name) = 'Event Secondary Samples ' then '55 Events'
        when concat(first_name, ' ', last_name) = 'Export Samples ' then '48 Export'
        when concat(first_name, ' ', last_name) = 'Inbound Pours' then '43 Inbound'
        when concat(first_name, ' ', last_name) = 'Marketing Donations ' then '60 Marketing'
        when concat(first_name, ' ', last_name) = 'Marketing Samples ' then '60 Marketing'
        when concat(first_name, ' ', last_name) = 'Marketing Secondary Samples ' then '60 Marketing'
        when concat(first_name, ' ', last_name) = 'Ownership Damaged ' then '88 Art/Shareholder'
        when concat(first_name, ' ', last_name) = 'Ownership Samples ' then '88 Art/Shareholder'
        when concat(first_name, ' ', last_name) = 'Ownership Secondary Samples ' then '88 Art/Shareholder'
        when concat(first_name, ' ', last_name) = 'Production Samples ' then '30 Production'
        when concat(first_name, ' ', last_name) = 'Tasting Room Damaged ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Tasting Room Donations ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Tasting Room Employee Comp ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Tasting Room Pours ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Tasting Room Samples ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Tasting Room Secondary Samples ' then '50 TR'
        when concat(first_name, ' ', last_name) = 'Wine Club Pours ' then '54 Wine Club'
        when concat(first_name, ' ', last_name) = 'Wine Club Samples ' then '54 Wine Club'
        when concat(first_name, ' ', last_name) = 'Wine Club Secondary Samples ' then '54 Wine Club'
        else null
    end as no_charge_class
from base
order by concat(first_name, ' ', last_name) asc