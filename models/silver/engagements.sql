{#
-- Description: Incremental Load Script for Silver Layer - engagements Table
-- Script Name: silver_engagements.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the engagements table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_engagement_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'engagements') }}   -- adjust if your bronze source/table name differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}                 as hs_engagement_id,

    -- DETAILS
    {{ clean_string('engagement_type') }}    as engagement_type,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('createdate') }}   as created_date,

    -- RELATIONSHIPS
    {{ clean_string('owner_id') }}           as owner_id,
    {{ clean_string('contact_id') }}         as contact_id,
    {{ clean_string('company_id') }}         as company_id,

    -- CONTENT
    {{ clean_string('subject') }}            as subject,
    {{ clean_string('body') }}               as body,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz       as silver_load_date

  from raw
),

final as (

  select
    hs_engagement_id   as hs_engagement_id,
    engagement_type    as engagement_type,
    created_date       as created_date,
    owner_id           as owner_id,
    contact_id         as contact_id,
    company_id         as company_id,
    subject            as subject,
    body               as body,
    silver_load_date   as silver_load_date
  from cleaned
)

select *
from final