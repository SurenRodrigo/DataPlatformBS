SELECT
    "Selskap"                               AS company,
    "Sted/avdeling"                         AS department,
    "Organisasjonsnr."                      AS organization_number,
    "Intern_Plade_ID"                       AS internal_place_id,
    "Varenummer"                            AS item_number,
    "Varenavn lasting"                      AS item_name_loading,
    "Varenavn lossing"                      AS item_name_unloading,
    "Pris"                                  AS price,
    "Enhet"                                 AS unit,
    "Intern_Item_ID"                        AS internal_item_id,
    "Artikkelnavn AdmMit"                   AS item_name_admmit,
    "Artikkelnavn AdmMitAlternativ"         AS admmit_alternative
FROM {{ source('raw_nrc_source', 'source_sharepoint_gk_compello_data') }}
