{% snapshot dim_deal %}
{{
    config(
        unique_key="hs_deal_id",
        strategy="timestamp",
        updated_at="last_modified_date",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

-- Snapshot raw source attributes only (no joins to gold dimensions).
select
    -- natural/business key
    hs_deal_id,

    -- attributes
    deal_name,
    amount,
    pipeline_id,           
    stage_id,             
    owner_id,             
    company_id,          
    deal_type,
    probability,

    -- source dates (raw)
    close_date,
    created_date,

    -- timestamps for SCD
    last_modified_date,   
    silver_load_date

from {{ ref("deals") }}

{% endsnapshot %}
``