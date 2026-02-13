{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='name',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with src as (
    select
        distinct
        c.type        as name,
        current_timestamp() as gold_load_date,

        -- incremental tracking
        coalesce(c.last_modified_date, c.created_date, c.silver_load_date) as last_modified_date

    from {{ ref('campaigns') }} c
    where c.type is not null
)

select
    name,
    gold_load_date,
    last_modified_date
from src