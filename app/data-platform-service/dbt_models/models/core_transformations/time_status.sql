with time_status_data as (
select
    hoursstatuscode,
    hoursstatusdescrsl,
    _key
from {{ source('raw_nrc_source', 'source_icore_hours_status') }}
)
    select 
        {{ dbt_utils.generate_surrogate_key(['tsd._key']) }} as id,
        tsd.hoursstatuscode as time_status_code,
        tsd.hoursstatusdescrsl as "description",
        tsd._key as time_status_key
    from time_status_data as tsd