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

-- User-maintained attributions, written by the dashboard's Inputs Manager
manual_attributions as (
    select
        experience,
        attribution
    from {{ source('manual', 'experience_attribution') }}
),

-- Preserve attributions set directly on this table before the manual source existed
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
        coalesce(ma.attribution, ea.attribution) as attribution
    from unique_experiences ue
    left join manual_attributions ma on ue.experience = ma.experience
    left join existing_attributions ea on ue.experience = ea.experience
    order by ue.experience
)

select * from experience_dimension
