{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_engagement_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  ) 
}}

with final as (
    select
        se.hs_engagement_id,

        -- dims
        o.hs_owner_id      as owner_id,
        c.hs_contact_id    as contact_id,
        co.hs_company_id   as company_id,
        dd.date_key        as created_date_key,

        -- descriptive fields
        se.subject,
        se.body            as notes,

        -- associated fks
        d.hs_deal_id       as associated_deal_key,
        t.hs_ticket_id     as associated_ticket_key,

        -- audit / watermark
        se.created_date,
        o.last_modified_date   as last_modified_date

    from {{ ref('engagements') }} se
    left join {{ ref('dim_owner') }}   o
      on se.owner_id   = o.hs_owner_id 
    left join {{ ref('dim_contact') }} c
      on se.contact_id = c.hs_contact_id 
    left join {{ ref('dim_company') }} co
      on se.company_id = co.hs_company_id 
    left join {{ ref('dim_dates') }} dd
      on dd.calendar_date = cast(se.created_date as date)
    left join {{ ref('dim_deal') }} d
      on se.owner_id = d.owner_id
    left join {{ ref('dim_ticket') }} t
      on se.owner_id = t.owner_id 

    {% if is_incremental() %}
      where o.last_modified_date >
        (
          select coalesce(
              max(tgt.last_modified_date),
              '1900-01-01'::timestamp_ntz
          )
          from {{ this }} tgt
        )
    {% endif %}
)

select
    hs_engagement_id,
    owner_id,
    contact_id,
    company_id,
    created_date_key,
    subject,
    notes,
    associated_deal_key,
    associated_ticket_key,
    last_modified_date
from final
