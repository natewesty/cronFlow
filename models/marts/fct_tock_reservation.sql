{{
  config(
    materialized='table'
  )
}}

with source_data as (
    select * from {{ ref('stg_tock_reservation') }}
),

payment_data as (
    select
        tock_reservation_id,
        -- Extract payment information from the payments JSON array
        case 
            when jsonb_array_length(payments) > 0 
            then (payments->0->>'tockFeeCents')::int 
            else 0 
        end as tock_fee_cents,
        case 
            when jsonb_array_length(payments) > 0 
            then (payments->0->>'processorFeeCents')::int 
            else 0 
        end as processor_fee_cents
    from source_data
),

reservation_mart as (
    select
        -- Basic reservation information
        sd.tock_reservation_id,
        
        -- Diner patron information (renamed columns)
        sd.diner_patron_id as patron_id,
        sd.diner_patron_first_name as first_name,
        sd.diner_patron_last_name as last_name,
        sd.diner_patron_email as email,
        
        -- Reservation details
        to_char(
            to_timestamp(sd.reservation_datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), 
            'MM-DD-YYYY'
        ) as reservation_datetime,
        sd.party_size,
        sd.experience_name,
        sd.party_state as status,
        
        -- Pricing information (converted from cents to dollars)
        round(sd.net_amount_paid_cents / 100.0, 2) as subtotal,
        
        -- Payment fees (converted from cents to dollars)
        round(pd.tock_fee_cents / 100.0, 2) as tock_fee,
        round(pd.processor_fee_cents / 100.0, 2) as processor_fee,
        
        -- Calculate final total
        round(
            (sd.net_amount_paid_cents / 100.0) - 
            (pd.tock_fee_cents / 100.0) - 
            (pd.processor_fee_cents / 100.0), 
            2
        ) as final_total
        
    from source_data sd
    left join payment_data pd on sd.tock_reservation_id = pd.tock_reservation_id
    where sd.party_state != 'CANCELLED'
)

select * from reservation_mart
