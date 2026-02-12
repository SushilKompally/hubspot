{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='activity_unique_key',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  )
}}

WITH source_rows AS (

    SELECT
        -- PRIMARY KEY (SURROGATE)
        {{ dbt_utils.surrogate_key([
            'da.hs_deal_id',
            'fe.engagement_key',
            'dd.date_key'
        ]) }} AS activity_unique_key,

        -- FOREIGN KEYS
        da.hs_deal_id      AS deal_key,
        fe.engagement_key  AS activity_key,
        dd.date_key        AS date_key,

        -- AUDIT / METADATA
        da.last_modified_date

    FROM {{ ref('deals') }} da
    JOIN {{ ref('fact_engagement') }} fe
         ON da.company_id = fe.company_key
    JOIN {{ ref('dim_dates') }} dd
         ON dd.calendar_date = CAST(da.created_date AS DATE)

    {% if is_incremental() %}
    -- Only include new or changed data
    WHERE CAST(da.last_modified_date AS TIMESTAMP_NTZ) > (
        SELECT COALESCE(MAX(last_modified_date), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}

)

-- dbt uses this SELECT for MERGE
SELECT
    activity_unique_key,
    deal_key,
    activity_key,
    date_key,
    last_modified_date
FROM source_rows