{#
-- Description: Incremental Load Script for Silver Layer - contacts Table
-- Script Name: silver_contacts.sql
-- Created on: 6-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the contacts table.
-- Data source version: v62.0
-- Change History:
--     6-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key = 'hs_contact_id',
    incremental_strategy = 'merge',
    materialized = 'table',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('hubspot_bronze', 'contacts') }}
  where 1 = 1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('id') }}                   as hs_contact_id,

    -- PERSON DETAILS
    {{ clean_string('firstname') }}            as first_name,
    {{ clean_string('middlename') }}           as middle_name,
    {{ clean_string('lastname') }}             as last_name,
    {{ clean_string('email') }}                as email,
    {{ clean_string('phone') }}                as phone,
    {{ clean_string('mobilephone') }}          as mobile_phone,
    {{ clean_string('lifecycle_stage') }}      as lifecycle_stage,
    {{ clean_string('hubspot_owner_id') }}     as owner_id,
    {{ clean_string('company') }}              as company_name,
    {{ clean_string('jobtitle') }}             as job_title,

    {{ clean_string('lead_status') }}       as lead_status,

    -- ADDRESS
    {{ clean_string('street_address') }}       as address,
    {{ clean_string('city') }}                 as city,
    {{ clean_string('state') }}                as state,
    {{ clean_string('postal_code') }}          as postal_code,
    {{ clean_string('country') }}              as country,

    -- MISC
    {{ clean_string('fax') }}                  as fax,
    {{ clean_string('timezone') }}             as timezone,
    {{ clean_string('time_zone_offset') }}      as time_zone_offset,

    -- METRICS
    lifetime_value,

    -- DATES / TIMESTAMPS
    {{ safe_timestamp_ntz('createdate') }}         as created_date,
    {{ safe_timestamp_ntz('lastmodifieddate') }}   as last_modified_date,

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz         as silver_load_date

  from raw
),

final as (

  select
    -- final projection matches YAML (1:1 names and order is okay but not required)
    hs_contact_id       as hs_contact_id,
    first_name          as first_name,
    middle_name         as middle_name,
    last_name           as last_name,
    email               as email,
    phone               as phone,
    mobile_phone        as mobile_phone,
    lifecycle_stage     as lifecycle_stage,
    owner_id            as owner_id,
    company_name        as company_name,
    job_title           as job_title,
    lead_status         as lead_status,
    address             as address,
    city                as city,
    state               as state,
    postal_code         as postal_code,
    country             as country,
    fax                 as fax,
    timezone            as timezone,
    time_zone_offset    as time_zone_offset,
    lifetime_value      as lifetime_value,
    created_date        as created_date,
    last_modified_date  as last_modified_date,
    silver_load_date    as silver_load_date
  from cleaned
)

select *
from final