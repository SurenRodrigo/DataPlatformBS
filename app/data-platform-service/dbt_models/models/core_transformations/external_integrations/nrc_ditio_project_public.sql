{{ config(materialized='table') }}

/*
Exposes Ditio project data (from int__nrc_ditio_project) into the public schema
so that it can be accessed via Hasura/GraphQL.
*/

select
    id,
    tenant_id,
    tenant_name,
    company_id,
    ext_company_id,
    company_name,
    ext_project_id,
    project_name,
    project_number,
    project_identifier,
    ext_project_guid,
    external_number
from {{ ref('int__nrc_ditio_project') }}

