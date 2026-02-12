{#
-- Description: Incremental Load Script for Silver Layer - lead history Table
-- Script Name: silver_leadhistory.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the lead history table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_history_id',
    materialized = 'table',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'lead_history') }}   -- adjust if the bronze table name differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('history_id') }}             as hs_history_id,

    -- RELATIONS
    {{ clean_string('lead_id') }}                as lead_id,

    -- CHANGE ATTRIBUTES
    {{ clean_string('status') }}                 as status,
    {{ clean_string('source') }}                 as source,
    {{ clean_string('old_value') }}              as old_value,
    {{ clean_string('new_value') }}              as new_value,
    {{ clean_string('changed_by') }}             as changed_by,
    {{ clean_string('change_type') }}            as change_type,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('change_date') }}      as change_date,
    {{ safe_timestamp_ntz('created_date') }}     as created_date,
    -- As per your projection, modified_date = change_date
    {{ safe_timestamp_ntz('change_date') }}      as modified_date,

    -- FLAGS (normalize to boolean)
    case
      when {{ clean_string('is_current') }} in ('1','true','t','yes','y') then true
      when {{ clean_string('is_current') }} in ('0','false','f','no','n') then false
      else try_to_boolean({{ clean_string('is_current') }})
    end                                           as is_current,

    case
      when {{ clean_string('is_deleted') }} in ('1','true','t','yes','y') then true
      when {{ clean_string('is_deleted') }} in ('0','false','f','no','n') then false
      else try_to_boolean({{ clean_string('is_deleted') }})
    end                                           as is_deleted,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz           as silver_load_date

  from raw
),

final as (

  select
    hs_history_id     as hs_history_id,
    lead_id           as lead_id,
    status            as status,
    source            as source,
    old_value         as old_value,
    new_value         as new_value,
    changed_by        as changed_by,
    change_type       as change_type,
    change_date       as change_date,
    created_date      as created_date,
    is_current        as is_current,
    is_deleted        as is_deleted,
    modified_date     as modified_date,
    silver_load_date  as silver_load_date
  from cleaned
)

select *
from final