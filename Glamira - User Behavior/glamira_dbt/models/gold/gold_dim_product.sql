{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('silver_dim_product') }}
),

final AS (
    SELECT
        CAST(product_key AS INT64) AS product_key,
        product_id,
        name,
        category_id,
        product_type,
        sku,
        collection,

        ingested_at AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
    FROM base
),

unknown AS (
    SELECT
        -1 AS product_key,
        CAST(NULL AS INT64) AS product_id,
        'unknown' AS name,
        'unknown' AS category_id,
        'unknown' AS product_type,
        CAST(NULL AS STRING) AS sku,
        CAST(NULL AS STRING) AS collection,

        TIMESTAMP('1900-01-01') AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
)

SELECT * FROM final
UNION ALL
SELECT * FROM unknown