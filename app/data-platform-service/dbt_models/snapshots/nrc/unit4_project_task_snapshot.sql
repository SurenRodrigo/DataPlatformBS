{% snapshot unit4_project_task_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='task_identifier',
    strategy='timestamp',
    updated_at='last_updated_at::TIMESTAMP'
) }}

SELECT
    *,
    (lastupdated ->> 'updatedAt')::TIMESTAMP AS last_updated_at,
    {{ dbt_utils.generate_surrogate_key(['companyid', 'attributevalue']) }} AS task_identifier
FROM {{ source('raw_nrc_source', 'source_unit4_project_task') }}

{% endsnapshot %}