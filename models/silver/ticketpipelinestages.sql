{#
-- Description: Incremental Load Script for Silver Layer - ticket pipeline stages Table
-- Script Name: silver_ticketpipelinestages.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the ticket pipeline stages table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'stage_id',
    materialized = 'table',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'ticketpipelinestages') }}   -- adjust if bronze source/table differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}           as stage_id,

    -- DETAILS
    {{ clean_string('label') }}        as label,

    -- RELATION
    {{ clean_string('pipeline_id') }}  as pipeline_id,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz as silver_load_date

  from raw
),

final as (

  select
    stage_id         as stage_id,
    label            as label,
    pipeline_id      as pipeline_id,
    silver_load_date as silver_load_date
  from cleaned
)

select *
from final