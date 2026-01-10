{% snapshot unit4_project_portfolio_snapshot %}

{{ 
    config(
        target_schema='snapshots',
        unique_key='client || period || projectno',
        strategy='check',
        check_cols=[
            'accbidrag',
            'accebit',
            'accest',
            'accestbidrag',
            'accinnt',
            'accinv',
            'client',
            'divisjon',
            'invkap',
            'orderstock',
            'period',
            'periodbidrag',
            'periodebit',
            'periodinnt',
            'periodinv',
            'project',
            'projectno',
            'protyp',
            'protyptext',
            'status',
            'yearinv',
            'ytdbidrag',
            'ytdebit',
            'ytdest',
            'ytdestbidrag',
            'ytdinnt'
        ]
    ) 
}}

SELECT
    *
FROM {{ source('raw_nrc_source', 'source_unit4_project_portfolio') }}

{% endsnapshot %}