{% snapshot dim_company %}
{{
    config(
        unique_key="hs_company_id",
        strategy="timestamp",
        updated_at="last_modified_date",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    hs_company_id,

    -- attributes
    name,
    domain,
    phone,
    industry,
    address,
    city,
    state,
    postal_code,
    country,
    owner_id,          
    lifecycle_stage,

    -- audit fields from source
    last_modified_date, 
    created_date,
    silver_load_date

from {{ ref("companies") }}

{% endsnapshot %}