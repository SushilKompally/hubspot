{#
-- Description: Incremental Load Script for Silver Layer - campaign Table
-- Script Name: silver_campaign.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the campaign table.
-- Data source version:v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}



{{ config(
    unique_key='campaign_id',
    incremental_strategy='merge',
    materialized = 'table',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'campaign') }}
  where 1=1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }} as campaign_id,

    -- DETAILS 
    {{ clean_string('owner') }} as owner,
    {{ clean_string('status') }} as status,
    {{ clean_string('goal') }} as goal,

    -- DATES
    {{ safe_date('start_date') }} as start_date,
    {{ safe_date('end_date') }} as end_date,
    {{ safe_date('last_updated') }} as last_modified_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz as silver_load_date

  from raw
),

final as (

  select
    -- Map to requested final projection 
    campaign_id as hs_campaign_id,
    owner       as owner,
    end_date    as end_date,
    start_date  as start_date,
    status      as status,
    goal        as goal,
    last_modified_date as last_modified_date, 
    silver_load_date as silver_load_date
  from cleaned
)

select *
from final