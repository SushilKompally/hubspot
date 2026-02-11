{#
-- Description: Incremental Load Script for Silver Layer - line items Table
-- Script Name: silver_lineitems.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the line items table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_lineitem_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'lineitems') }}  -- adjust schema/table if needed
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}              as hs_lineitem_id,

    -- DETAILS
    {{ clean_string('name') }}            as name,

    -- METRICS
    {{ safe_decimal('price') }}            as price,
    quantity,

    -- RELATIONS
    {{ clean_string('product_id') }}      as product_id,
    {{ clean_string('deal_id') }}         as deal_id,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz    as silver_load_date

  from raw
),

final as (

  select
    hs_lineitem_id    as hs_lineitem_id,
    name              as name,
    price             as price,
    quantity          as quantity,
    product_id        as product_id,
    deal_id           as deal_id,
    silver_load_date  as silver_load_date
  from cleaned
)

select *
from final