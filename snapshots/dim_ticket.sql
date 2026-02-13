{% snapshot dim_ticket %}

{{
    config(
        unique_key="hs_ticket_id",
        strategy="timestamp",
        updated_at="last_modified_date",   
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- natural key
    hs_ticket_id,

    -- attributes
    subject,
    created_date,          -- raw timestamp (gold model will derive created_date_key)
    closed_date,           -- raw timestamp (gold model will derive closed_date_key)
    owner_id,              -- raw owner id (maps to dim_owner)
    pipeline_id,           -- raw pipeline id (maps to dim_ticketpipeline)
    stage_id,              -- raw stage id (maps to dim_ticket_pipeline_stage)
    priority,
    status,
    company_id,            -- raw company id (maps to dim_company)

    -- source/system timestamps
    last_modified_date,    -- used as updated_at for SCD2 snapshot
    created_date as created_timestamp,  -- optional alias if you want both
    silver_load_date

from {{ ref("tickets") }}

{% endsnapshot %}