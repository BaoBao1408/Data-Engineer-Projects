{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('silver_dim_location') }}
)

SELECT
    CAST(location_key AS INT64) AS location_key,
    INITCAP(country) AS country,
    INITCAP(region) AS region,
    INITCAP(city) AS city,

    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    CURRENT_TIMESTAMP() AS ingested_at

FROM src