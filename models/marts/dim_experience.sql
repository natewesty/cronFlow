{{
  config(
    materialized='table',
    on_schema_change='append_new_columns'
  )
}}

with unique_experiences as (
    select distinct
        experience_name as experience
    from {{ ref('stg_tock_reservation') }}
    where experience_name is not null
),

-- Get existing attributions to preserve manual entries
existing_attributions as (
    select 
        experience,
        attribution
    from {{ this }}
    where attribution is not null
),

experience_dimension as (
    select
        ue.experience,
        coalesce(ea.attribution, null::varchar) as attribution
    from unique_experiences ue
    left join existing_attributions ea on ue.experience = ea.experience
    order by ue.experience
)

select * from experience_dimension
