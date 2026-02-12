{#
-- Description: Incremental Load Script for Silver Layer - tickets Table
-- Script Name: silver_tickets.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the tickets table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_ticket_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'tickets') }}   -- adjust if your bronze source/table name differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}                     as hs_ticket_id,

    -- DETAILS
    {{ clean_string('subject') }}                as subject,
    {{ clean_string('content') }}                as content,
    {{ clean_string('priority') }}               as priority,
    {{ clean_string('source') }}                 as source,
    {{ clean_string('status') }}                 as status,

    -- RELATIONSHIPS
    {{ clean_string('hubspot_owner_id') }}       as owner_id,
    {{ clean_string('ticket_pipeline') }}        as pipeline_id,
    {{ clean_string('ticket_stage') }}           as stage_id,
    {{ clean_string('associatedcompanyid') }}    as company_id,

    -- contact_ids can be array/CSV in source; preserve as cleaned string in silver
    {{ clean_string('associatedcontactids') }}   as contact_ids,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('createdate') }}       as created_date,
    {{ safe_timestamp_ntz('closedate') }}        as closed_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz           as silver_load_date

  from raw
),

final as (

  select
    hs_ticket_id      as hs_ticket_id,
    subject           as subject,
    content           as content,
    created_date      as created_date,
    closed_date       as closed_date,
    owner_id          as owner_id,
    pipeline_id       as pipeline_id,
    stage_id          as stage_id,
    priority          as priority,
    source            as source,
    company_id        as company_id,
    contact_ids       as contact_ids,
    status            as status,
    silver_load_date  as silver_load_date
  from cleaned
)

select *
from final