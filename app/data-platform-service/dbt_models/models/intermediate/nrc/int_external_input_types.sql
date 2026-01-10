SELECT
    "ExternalInputTypeID" AS external_input_type_id,
    "TenantID"            AS tenant_id,
    "InputName"           AS input_name,
    "Description"         AS description,
    "isActive"            AS is_active
FROM {{ ref('external_input_types_seed') }}