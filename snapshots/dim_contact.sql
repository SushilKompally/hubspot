{% snapshot dim_contact %}
{{
    config(
        unique_key="hs_contact_id",
        strategy="timestamp",
        updated_at="last_modified_date",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- natural key
    hs_contact_id,

    -- attributes
    first_name,
    middle_name,
    last_name,
    email,
    phone,
    mobile_phone,
    lifecycle_stage,
    owner_id,           -- raw natural owner id from silver
    company_name,

    -- source timestamps
    last_modified_date,
    created_date,
    silver_load_date

from {{ ref("contacts") }}

{% endsnapshot %}