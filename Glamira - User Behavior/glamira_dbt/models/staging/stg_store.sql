{{ config(materialized='view') }}

SELECT
    TRIM(store_code) AS store_code,
    TRIM(currency) AS currency,
    TRIM(region) AS region,
    TRIM(language) AS language,
    TRIM(country) AS country,
    TRIM(base_store) AS base_store
FROM {{ source('glamira_raw', 'dim_store') }}
WHERE store_code IS NOT NULL