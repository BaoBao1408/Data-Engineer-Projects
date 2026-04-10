SELECT DISTINCT
    store_code
    ,currency
    ,region
    ,language
    ,country
    ,base_store

FROM {{ source('glamira_raw', 'dim_store') }}