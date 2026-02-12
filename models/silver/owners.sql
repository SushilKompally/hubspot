{#
-- Description: Incremental Load Script for Silver Layer - owners Table
-- Script Name: silver_owners.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the owners table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_owner_id',
    incremental_strategy = 'merge',
    materialized = 'table',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'owners') }}   -- adjust if bronze source/table name differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}           as hs_owner_id,

    -- DETAILS
    {{ clean_string('first_name') }}   as first_name,
    {{ clean_string('last_name') }}    as last_name,
    {{ clean_string('email') }}        as email,
    {{ clean_string('user_role') }}    as user_role,
    {{ clean_string('team_id') }}      as team_id,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz as silver_load_date

  from raw
),

final as (

  select
    hs_owner_id      as hs_owner_id,
    first_name       as first_name,
    last_name        as last_name,
    email            as email,
    user_role        as user_role,
    team_id          as team_id,
    silver_load_date as silver_load_date
  from cleaned
)

select *
from final