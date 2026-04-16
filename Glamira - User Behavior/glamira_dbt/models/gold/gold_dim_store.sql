{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('silver_dim_store') }}
),

dedup AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY store_key
                ORDER BY ingested_at DESC
            ) AS rn
        FROM src
        WHERE store_id IS NOT NULL
    )
    WHERE rn = 1
),

final AS (
    SELECT
        CAST(store_key AS INT64) AS store_key,
        CAST(store_id AS INT64) AS store_id,
        store_code,

        UPPER(currency) AS currency,
        INITCAP(region) AS region,
        LOWER(language) AS language,
        INITCAP(country) AS country,

        base_store,

        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        ingested_at
    FROM dedup
),

unknown AS (
    SELECT
        -1 AS store_key,
        CAST(NULL AS INT64) AS store_id,
        'unknown' AS store_code,

        'unknown' AS currency,
        'unknown' AS region,
        'unknown' AS language,
        'unknown' AS country,

        'unknown' AS base_store,

        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        CURRENT_TIMESTAMP() AS ingested_at
)

SELECT * FROM final
WHERE store_key != -1
UNION ALL
SELECT * FROM unknown