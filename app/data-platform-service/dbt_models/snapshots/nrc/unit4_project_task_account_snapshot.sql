{% snapshot unit4_project_task_account_snapshot %} 
 
{{ 
  config( 
    target_schema = 'snapshots',  
    unique_key    = 'client || \'-\' || period || \'-\' || project || \'-\' || prodkode || \'-\' || kode', 
    strategy      = 'check', 
    check_cols    = ['amountacc', 'amountytd', 'amountperiod', 'prodkodenavn'] 
  ) 
}} 
 
SELECT 
  * 
FROM {{ source('raw_nrc_source', 'source_unit4_project_task_account') }} 
 
{% endsnapshot %}