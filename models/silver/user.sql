{#
-- Description: Incremental Load Script for Silver Layer - users Table
-- Script Name: silver_users.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the users table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_user_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'users') }}   -- adjust if your bronze source/table differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}           as hs_user_id,

    -- DETAILS
    {{ clean_string('email') }}        as email,
    {{ clean_string('first_name') }}   as first_name,
    {{ clean_string('last_name') }}    as last_name,
    {{ clean_string('owner_id') }}     as owner_id,
    {{ clean_string('description') }}  as description,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz as silver_load_date

  from raw
),

final as (

  select
    hs_user_id       as hs_user_id,
    email            as email,
    first_name       as first_name,
    last_name        as last_name,
    owner_id         as owner_id,
    description      as description,
    silver_load_date as silver_load_date
  from cleaned
)

select *
from final