{{ config(enabled=false) }}
SELECT DISTINCT
    ip
    ,country
    ,region
    ,city

FROM {{ source('glamira_raw', 'dim_location') }}