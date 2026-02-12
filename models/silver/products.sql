{#
-- Description: Incremental Load Script for Silver Layer - products Table
-- Script Name: silver_products.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the products table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_product_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'products') }}  -- adjust if your bronze source/table differs
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}                 as hs_product_id,

    -- DETAILS
    {{ clean_string('name') }}               as name,
    {{ clean_string('sku') }}                as sku,

    -- METRICS
    {{ safe_decimal('price') }}               as price,

    -- OTHER
    {{ clean_string('description') }}        as description,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('createdate') }}   as created_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz       as silver_load_date

  from raw
),

final as (

  select
    hs_product_id     as hs_product_id,
    name              as name,
    sku               as sku,
    price             as price,
    description       as description,
    created_date      as created_date,
    silver_load_date  as silver_load_date
  from cleaned
)

select *
from final