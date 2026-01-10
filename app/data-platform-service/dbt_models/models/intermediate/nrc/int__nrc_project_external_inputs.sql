WITH hse_inputs AS (
    SELECT
        "Year"                                       AS year,
        TO_CHAR(TO_DATE("Month"::TEXT, 'MM'), 'MON') AS month,
        "ProjectNumber"::TEXT                        AS project_number,
        "NUM_ABSENCE_DAYS_PERSONAL_INJURY"           AS num_absence_days_personal_injury,
        "NUM_DELIVERED_TONS_RESIDUAL_WASTE"          AS num_delivered_tons_residual_waste,
        "NUM_HRS_RENTED_WORKERS_HSE"                 AS num_hrs_rented_workers_hse,
        "NUM_HRS_SUBCONTRACTOR_HSE"                  AS num_hrs_subcontractor_hse,
        "TOTAL_AMOUNT_DELIVERED_WASTE"               AS total_amount_delivered_waste,
        "LastModified"                               AS last_modified
    FROM {{ source('raw_nrc_source', 'source_hse_project_external_inputs') }}
),

unpivoted AS (
    SELECT
        hse.year,
        hse.month,
        project.tenant_id::TEXT      AS tenant_id,
        project.company_id::TEXT     AS company_id,
        project.division_id,
        project.department_id,
        hse.project_number,
        vals.field,
        vals.value::NUMERIC          AS value,
        hse.last_modified::TIMESTAMP AS last_modified
    FROM hse_inputs AS hse
    LEFT JOIN {{ ref('project') }} AS project
        ON hse.project_number::INT = project.project_number
    CROSS JOIN LATERAL (
        VALUES
        ('NUM_ABSENCE_DAYS_PERSONAL_INJURY', hse.num_absence_days_personal_injury::TEXT),
        (
            'NUM_DELIVERED_TONS_RESIDUAL_WASTE',
            hse.num_delivered_tons_residual_waste::TEXT
        ),
        ('NUM_HRS_RENTED_WORKERS_HSE', hse.num_hrs_rented_workers_hse::TEXT),
        ('NUM_HRS_SUBCONTRACTOR_HSE', hse.num_hrs_subcontractor_hse::TEXT),
        ('TOTAL_AMOUNT_DELIVERED_WASTE', hse.total_amount_delivered_waste::TEXT)
    ) AS vals (field, value)
)

SELECT
    year,
    month,
    tenant_id,
    company_id,
    division_id,
    department_id,
    project_number,
    field,
    value,
    last_modified
FROM unpivoted
