SELECT
    customerid                                                  AS customer_id,
    companyid                                                   AS company_id,
    customername                                                AS name,
    companyregistrationnumber                                   AS company_reg_no,
    vatregistrationnumber                                       AS vat_reg_no,
    contactpoints
    -> 0
    -> 'address'
    ->> 'streetAddress'                                         AS address_street,
    contactpoints
    -> 0
    -> 'address'
    ->> 'place'                                                 AS address_place,
    contactpoints
    -> 0
    -> 'address'
    ->> 'postcode'                                              AS address_post_code,
    contactpoints
    -> 0
    -> 'address'
    ->> 'province'                                              AS address_province,
    contactpoints
    -> 0
    -> 'address'
    ->> 'countryCode'                                           AS address_country_code,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone1'                                            AS phone_number1,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone2'                                            AS phone_number2,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone3'                                            AS phone_number3,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone4'                                            AS phone_number4,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone5'                                            AS phone_number5,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone6'                                            AS phone_number6,
    contactpoints
    -> 0
    -> 'phoneNumbers'
    ->> 'telephone7'                                            AS phone_number7,
    contactpoints -> 0 -> 'additionalContactInfo' ->> 'eMail'  AS email,
    contactpoints
    -> 0
    -> 'additionalContactInfo'
    ->> 'contactPerson'                                         AS contact_person,
    contactpoints -> 0 -> 'additionalContactInfo' ->> 'url'     AS website_url,
    contactpoints
    -> 0
    -> 'additionalContactInfo'
    ->> 'contactPosition'                                       AS contact_position,
    aliasname                                                   AS alias_name,
    externalreference                                           AS external_reference,
    relatedvalues
    -> 0
    ->> 'unitValue'                                             AS related_values_unit_value,
    relatedvalues
    -> 0
    ->> 'percentage'                                            AS related_values_percentage,
    relatedvalues
    -> 0
    ->> 'relationId'                                            AS related_values_relation_id,
    relatedvalues
    -> 0
    ->> 'relatedValue'                                          AS related_values_related_value,
    relatedvalues
    -> 0
    ->> 'relationName'                                          AS related_values_relation_name,
    relatedvalues
    -> 0
    ->> 'relationGroup'                                         AS related_values_relation_group,
    payment ->> 'status'                                        AS payment_status,
    payment ->> 'paymentMethod'                                 AS payment_method,
    payment
    ->> 'debtCollectionCode'                                    AS payment_debt_collection_code,
    lastupdated
    ->> 'updatedAt'                                             AS last_modified_date,
    lastupdated ->> 'updatedBy'                                 AS last_modified_by,
    note                                                        AS description,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_customer_snapshot') }}
WHERE dbt_valid_to IS NULL
