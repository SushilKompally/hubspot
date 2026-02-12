{% snapshot dim_campaign %}
{{
    config(
        unique_key="hs_campaign_id",
        strategy="timestamp",
        updated_at="last_modified_date",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    hs_campaign_id,

    -- attributes
    owner            as name,
    status,
    start_date,
    end_date,
    owner            as owner_name,

    -- audit fields from source
    modified_date,
    created_date,
    silver_load_date

from {{ ref("campaign") }}

{% endsnapshot %}