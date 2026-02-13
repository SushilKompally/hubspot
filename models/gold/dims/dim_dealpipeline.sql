{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_pipeline_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with src as (
    select
        dp.hs_pipeline_id,
        dp.label,
        
        current_timestamp() as gold_load_date,

        -- handle timestamp drift in silver
        coalesce(dp.last_modified_date, dp.modified_date, dp.created_date, dp.silver_load_date)
            as last_modified_date

    from {{ ref('dealpipelines') }} dp
)

select
    hs_pipeline_id,
    label,
    gold_load_date,
    last_modified_date
from src