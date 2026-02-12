{#
-- Description: Incremental Load Script for Silver Layer - quotes Table
-- Script Name: silver_quotes.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the quotes table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_quote_id',
    incremental_strategy = 'merge',
    materialized = 'table',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'quotes') }}  -- adjust if your bronze schema/table differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}                  as hs_quote_id,

    -- DETAILS
    {{ clean_string('quote_name') }}          as name,

    -- RELATIONS
    {{ clean_string('deal_id') }}             as deal_id,

    -- METRICS
    {{ safe_decimal('amount') }}               as amount,


    -- STATUS
    {{ clean_string('status') }}              as status,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('createdate') }}    as created_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz        as silver_load_date

  from raw
),

final as (

  select
    hs_quote_id       as hs_quote_id,
    name              as name,
    deal_id           as deal_id,
    amount            as amount,
    status            as status,
    created_date      as created_date,
    silver_load_date  as silver_load_date
  from cleaned
)

select *
from final