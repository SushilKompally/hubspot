{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_lead_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with final as (
    select
        -- natural key
        sl.hs_lead_id,

        -- foreign keys
        o.hs_owner_id     as owner_id,
        c.hs_company_id   as company_id,

        -- attributes
        sl.status,
        sl.source,

        -- date keys
        dd1.date_key      as created_date_key,
        dd2.date_key      as modified_date_key,
        dd3.date_key      as converted_date_key,

        -- derived flags
        case when sl.status = 'Converted' then true else false end as is_converted,

        -- audit / lineage
        current_timestamp() as gold_load_date,
        coalesce(sl.modified_date, sl.created_date) as last_modified_date

    from {{ ref('lead') }} sl

    left join {{ ref('dim_owner') }} o
      on sl.lead_owner_id = o.hs_owner_id
     and o.is_current = true

    left join {{ ref('dim_company') }} c
      on sl.company_id = c.hs_company_id
     and c.is_current = true

    left join {{ ref('dim_dates') }} dd1
      on dd1.calendar_date = cast(sl.created_date as date)

    left join {{ ref('dim_dates') }} dd2
      on dd2.calendar_date = cast(sl.modified_date as date)

    left join {{ ref('dim_dates') }} dd3
      on dd3.calendar_date = cast(sl.modified_date as date)

    {% if is_incremental() %}
      where coalesce(sl.modified_date, sl.created_date) >
        (
          select coalesce(max(last_modified_date), '1900-01-01'::timestamp_ntz)
          from {{ this }}
        )
    {% endif %}
)

select
    hs_lead_id,
    owner_id,
    company_id,
    status,
    source,
    created_date_key,
    modified_date_key,
    converted_date_key,
    is_converted,
    gold_load_date,
    last_modified_date
from final