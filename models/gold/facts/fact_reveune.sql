{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['company_key', 'date_key', 'revenue_type'],
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

with final as (
    select
        dc.company_key,
        dd.date_key,
        sr.amount,
        sr.revenue_type,

        -- lineage
        current_timestamp() as gold_load_date,

        -- for incremental merge
        coalesce(sr.modified_date, sr.revenue_date) as last_modified_date

    from {{ ref('dim_company') }} sr       
    left join {{ ref('dim_company') }} dc
      on sr.company_id = dc.hs_company_id
     and dc.is_current = true
    left join {{ ref('dim_dates') }} dd
      on dd.calendar_date = cast(sr.revenue_date as date)

    {% if is_incremental() %}
      where coalesce(sr.modified_date, sr.revenue_date) >
        (
          select coalesce(max(last_modified_date), '1900-01-01'::timestamp_ntz)
          from {{ this }}
        )
    {% endif %}
)

select
    company_key,
    date_key,
    amount,
    revenue_type,
    gold_load_date,
    last_modified_date
from final