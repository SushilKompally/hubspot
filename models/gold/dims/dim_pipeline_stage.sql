{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_stage_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with src as (
    select
        distinct
        hs_stage_id,
        label,
        pipeline_id,
      --  probability,

        current_timestamp() as gold_load_date,

        -- incremental filter: use best available timestamp from silver
        silver_load_date as last_modified_date

    from {{ ref('dealpipelinestages') }} 
)

select
    hs_stage_id,
    label,
    pipeline_id,
   -- probability,
    gold_load_date,
    last_modified_date
from src