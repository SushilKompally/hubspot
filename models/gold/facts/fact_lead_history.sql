{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_history_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with final as (
    select
        h.hs_history_id,
        l.hs_lead_id       as lead_key,

        -- change details
        h.status,
        h.source,
        h.old_value,
        h.new_value,
        ---o.owner_key        as changed_by_key,
        h.change_type,
        h.change_date,
        h.created_date,

        -- scd flags
        h.is_current,
        h.is_deleted,

        -- lineage
        current_timestamp() as valid_from,

        -- incremental filter column
        h.change_date as last_modified_date

    from {{ ref('leadhistory') }} h

    left join {{ ref('fact_lead') }} l
      on h.lead_id = l.hs_lead_id

    left join  {{ ref('dim_owner') }} o
      on h.changed_by = o.hs_owner_id

    {% if is_incremental() %}
      where h.change_date >
        (
          select coalesce(max(last_modified_date), '1900-01-01'::timestamp_ntz)
          from {{ this }}
        )
    {% endif %}
)

select
    hs_history_id,
    lead_key,
    status,
    source,
    old_value,
    new_value,
   -- changed_by_key,
    change_type,
    change_date,
    created_date,
    is_current,
    is_deleted,
    valid_from,
    last_modified_date
from final