{{
  config(
    materialized='table'
  )
}}

with unique_experiences as (
    select distinct
        experience_name as experience
    from {{ ref('fct_tock_reservation') }}
    where experience_name is not null
),

experience_dimension as (
    select
        experience,
        null::varchar as attribution
    from unique_experiences
    order by experience
)

select * from experience_dimension
