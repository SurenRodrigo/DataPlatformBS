{{ config(materialized='table') }}

/*
This model creates a copy of the seed table `company_data_source_seed`
inside the `public` schema so that Hasura/GraphQL can expose it.
*/

select
    id,
    company_guid,
    tenant_id,
    data_source_id,
    ext_company_id
from {{ ref('company_data_source_seed') }}

