{#
-- Description: Incremental Load Script for Silver Layer - ticket pipelines Table
-- Script Name: silver_ticketpipelines.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the ticket pipelines table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'pipeline_id',
    materialized = 'table',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'ticketpipelines') }}   -- adjust if bronze source/table differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}     as pipeline_id,

    -- DETAILS
    {{ clean_string('label') }}  as label,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz as silver_load_date

  from raw
),

final as (

  select
    pipeline_id      as pipeline_id,
    label            as label,
    silver_load_date as silver_load_date
  from cleaned
)

select *
from final