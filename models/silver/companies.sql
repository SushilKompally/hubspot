{#
-- Description: Incremental Load Script for Silver Layer - companies Table
-- Script Name: silver_companies.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the companies table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_company_id',
    incremental_strategy = 'merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'companies') }}
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}                    as hs_company_id,

    -- DETAILS
    {{ clean_string('name') }}                  as name,
    {{ clean_string('domain') }}                as domain,
    {{ clean_string('phone') }}                 as phone,
    {{ clean_string('industry') }}              as industry,
    {{ clean_string('address') }}               as address,
    {{ clean_string('city') }}                  as city,
    {{ clean_string('state') }}                 as state,
    {{ clean_string('postal_code') }}           as postal_code,
    {{ clean_string('country') }}               as country,
    {{ clean_string('hubspot_owner_id') }}      as owner_id,
    {{ clean_string('company_type') }}          as company_type,

    -- NUMERIC
    {{ safe_decimal('annualrevenue') }}          as annual_revenue,
    numberofemployees     as employee_count,

    -- DATES / TIMESTAMPS (normalize; safe_timestamp_ntz handles blanks/invalids)
    {{ safe_timestamp_ntz('createdate') }}      as created_date,
    {{ safe_timestamp_ntz('lastmodifieddate') }} as last_modified_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz          as silver_load_date

  from raw
),

final as (

  select
    hs_company_id       as hs_company_id,
    name                as name,
    domain              as domain,
    phone               as phone,
    industry            as industry,
    address             as address,
    city                as city,
    state               as state,
    postal_code         as postal_code,
    country             as country,
    owner_id            as owner_id,
    created_date        as created_date,
    last_modified_date  as last_modified_date,
    company_type        as company_type,
    annual_revenue      as annual_revenue,
    employee_count      as employee_count,
    silver_load_date    as silver_load_date
  from cleaned
)

select *
from final