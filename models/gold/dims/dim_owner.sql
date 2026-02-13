{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_owner_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with src as (
    select
        -- natural/business key
        hs_owner_id,

        -- attributes
        first_name,
        last_name,
        email,
        team_id,

        -- convenience / denormalized fields
        trim(coalesce(first_name, '') || ' ' || coalesce(last_name, '')) as full_name,

        -- lineage
        current_timestamp() as gold_load_date,

        -- for incremental merge filter
        coalesce(last_modified_date, modified_date, created_date, silver_load_date) as last_modified_date

    from {{ ref('owners') }}
)

select
    hs_owner_id,
    first_name,
    last_name,
    email,
    team_id,
    full_name,
    gold_load_date,
    last_modified_date
from src
