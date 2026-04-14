{{ config(materialized='view') }}

WITH source_data AS (

    SELECT *
    FROM {{ source('glamira_raw', 'dim_product') }}

),

cleaned AS (

    SELECT
        SAFE_CAST(product_id AS INT64) AS product_id,
        TRIM(name) AS name,

        -- category (giữ string, chưa cần dim riêng)
        TRIM(category_name) AS category_name,

        TRIM(product_type) AS product_type,
        TRIM(sku) AS sku,
        TRIM(collection) AS collection,

        SAFE_CAST(price AS FLOAT64) AS price,
        SAFE_CAST(min_price AS FLOAT64) AS min_price,
        SAFE_CAST(max_price AS FLOAT64) AS max_price,

        TRIM(currency) AS currency,
        TRIM(store_code) AS store_code,

        -- optional
        SAFE_CAST(gender AS STRING) AS gender

    FROM source_data

    WHERE product_id IS NOT NULL

),

deduplicated AS (

    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY price DESC
            ) AS rn
        FROM cleaned
    )
    WHERE rn = 1

)

SELECT * EXCEPT(rn)
FROM deduplicated