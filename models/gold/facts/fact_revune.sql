{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hs_quote_id',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with final as (
    select
        -- primary/natural key
        sl.hs_quote_id,

        -- foreign keys
        sl.deal_id,
        dd.hs_deal_id as deal_key,

        -- attributes
        sl.name,
        sl.status,
        sl.amount,

        -- date keys
        d1.date_key as created_date_key,
        d2.date_key as closed_date_key,   
        -- incremental filter
        coalesce(sl.modified_date, sl.created_date) as last_modified_date

    from {{ ref('quotes') }} sl
    left join {{ ref('dim_deal') }} dd
      on sl.deal_id = dd.hs_deal_id
     and dd.is_current = true
    left join {{ ref('dim_date') }} d1
      on d1.calendar_date = cast(sl.created_date as date)
    left join {{ ref('dim_date') }} d2
      on d2.calendar_date = cast(sl.created_date as date)
    {% if is_incremental() %}
      where coalesce(sl.modified_date, sl.created_date) >
        (
          select coalesce(max(last_modified_date), '1900-01-01'::timestamp_ntz)
          from {{ this }}
        )
    {% endif %}
)

select
    hs_quote_id,
    deal_id,
    deal_key,
    name,
    status,
    amount,
    created_date_key,
    closed_date_key,
    gold_load_date,
    last_modified_date
from final