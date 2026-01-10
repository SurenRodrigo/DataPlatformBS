{% snapshot endringer_project_snapshot %} 
 
{{ 
  config( 
    target_schema = 'snapshots',  
    unique_key    = 'id', 
    strategy      = 'check', 
    check_cols    = ['name', 'projectnumber', 'organizationid', 'isfinished'] 
  ) 
}} 
 
SELECT 
  * 
FROM {{ source('raw_nrc_source', 'source_endringer_project') }} 
{% endsnapshot %}