{% snapshot endringer_amount_snapshot %} 
 
{{ 
  config( 
    target_schema = 'snapshots',  
    unique_key    = ' projectid ||  casetype ||  month || year ', 
    strategy      = 'check', 
    check_cols    = ['totalamount', 'totalacceptedamount', 'totaldeclinedamount', 'totalnothandledamount'] 
  ) 
}} 
 
SELECT 
  * 
FROM {{ source('raw_nrc_source', 'source_endringer_amount') }} 
{% endsnapshot %}