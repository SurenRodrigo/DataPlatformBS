WITH profitability_detail AS (
    SELECT *
    FROM {{ ref('dim_gunnar_knutsen_admmit_profitability_detail') }}
),

projects AS (
    SELECT
        ext_project_id,
        organizational_unit_id
    FROM {{ ref('dim_gunnar_knutsen_admmit_project') }}
),

departments AS (
    SELECT
        external_id              AS department_id,
        department_name
    FROM {{ ref('int__department_external_id_mapping') }}
)

SELECT
    departments.department_name,
    profitability_detail.project_name,
    profitability_detail.load_place_id,
    profitability_detail.unload_place_id,
    profitability_detail.date,
    profitability_detail.article,
    profitability_detail.unit_name,
    SUM(profitability_detail.project_settlement_surcharge)   AS resultat_kr,
    COUNT(*)                                                 AS number_of_loads,
    CASE
        WHEN SUM(profitability_detail.settlement) = 0 THEN NULL
        ELSE (SUM(profitability_detail.project_settlement_surcharge) / SUM(profitability_detail.settlement)) * 100
    END                                   AS margin_percentage
FROM profitability_detail
LEFT JOIN projects ON profitability_detail.project_id = projects.ext_project_id
LEFT JOIN departments ON projects.organizational_unit_id = departments.department_id
GROUP BY
    profitability_detail.project_id,
    profitability_detail.project_name,
    profitability_detail.load_place_id,
    profitability_detail.unload_place_id,
    profitability_detail.article,
    profitability_detail.date,
    profitability_detail.unit_name,
    departments.department_name
