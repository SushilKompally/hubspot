{#
-- Description: Incremental Load Script for Silver Layer - deals Table
-- Script Name: silver_deals.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the deals table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    materialized = 'incremental',
    unique_key = 'hs_deal_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'deals') }}
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- primary key
    {{ clean_string('id') }}                         as hs_deal_id,

    -- details
    {{ clean_string('dealname') }}                   as deal_name,
    {{ safe_decimal('amount') }}                      as amount,
    {{ clean_string('pipeline') }}                   as pipeline_id,
    {{ clean_string('dealstage') }}                  as stage_id,

    -- dates / timestamps
    {{ safe_date('closedate') }}                     as close_date,
    {{ safe_timestamp_ntz('createdate') }}           as created_date,
    {{ safe_timestamp_ntz('lastmodifieddate') }}     as last_modified_date,

    -- ownership / relationships
    {{ clean_string('hubspot_owner_id') }}           as owner_id,
    {{ clean_string('associatedcompanyid') }}        as company_id,

    -- classification / metrics
    {{ clean_string('dealtype') }}                   as deal_type,
    {{ safe_decimal('forecastamount') }}              as forecast_amount,
    ---{{ safe_integer('probability') }}                 as probability,

    -- other
    {{ clean_string('description') }}                as description,

    -- load / audit
    current_timestamp()::timestamp_ntz               as silver_load_date

  from raw
),

final as (

  select
    hs_deal_id,
    deal_name,
    amount,
    pipeline_id,
    stage_id,
    close_date,
    created_date,
    last_modified_date,
    owner_id,
    company_id,
    deal_type,
    forecast_amount,
    --probability,
    description,
    silver_load_date
  from cleaned
)

select *
from final

