{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_location') }}
)

SELECT
    location_key,
    country,
    region,
    city
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY location_key
            ORDER BY ip   -- arbitrary
        ) AS rn
    FROM {{ ref('stg_location') }}
)
WHERE rn = 1