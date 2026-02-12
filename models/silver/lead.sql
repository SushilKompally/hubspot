{#
-- Description: Incremental Load Script for Silver Layer - leads Table
-- Script Name: silver_leads.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the leads table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_lead_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'lead') }}   -- adjust if bronze table name differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('lead_id') }}        as hs_lead_id,

    -- OWNERSHIP / RELATIONSHIPS
    {{ clean_string('lead_owner_id') }}  as lead_owner_id,
    {{ clean_string('company_id') }}     as company_id,

    -- PERSON DETAILS
    {{ clean_string('first_name') }}     as first_name,
    {{ clean_string('last_name') }}      as last_name,
    {{ clean_string('email') }}          as email,
    {{ clean_string('phone') }}          as phone,

    -- STATUS / SOURCE
    {{ clean_string('status') }}         as status,
    {{ clean_string('source') }}         as source,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('created_date') }}   as created_date,
    {{ safe_timestamp_ntz('modified_date') }}  as modified_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz   as silver_load_date

  from raw
),

final as (

  select
    hs_lead_id        as hs_lead_id,
    lead_owner_id     as lead_owner_id,
    company_id        as company_id,
    first_name        as first_name,
    last_name         as last_name,
    email             as email,
    phone             as phone,
    status            as status,
    source            as source,
    created_date      as created_date,
    modified_date     as modified_date,
    silver_load_date  as silver_load_date
  from cleaned
)

select *
from final