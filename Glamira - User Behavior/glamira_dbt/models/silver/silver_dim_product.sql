{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_product') }}
),

currency_map AS (
    SELECT *
    FROM {{ ref('domain_currency') }}
),

clean AS (
    SELECT
        product_id,
        TRIM(name) AS name,
        LOWER(TRIM(category_name)) AS category_id,
        LOWER(TRIM(product_type)) AS product_type,
        TRIM(sku) AS sku,
        TRIM(collection) AS collection,

        SAFE_CAST(price AS FLOAT64) AS price,
        SAFE_CAST(min_price AS FLOAT64) AS min_price,
        SAFE_CAST(max_price AS FLOAT64) AS max_price,

        UPPER(currency) AS raw_currency,
        TRIM(store_code) AS store_code,

        -- 🎯 normalize store_code → base_store
        REGEXP_EXTRACT(store_code, r'^([a-z]+)') AS base_store,

        CURRENT_TIMESTAMP() AS ingested_at

    FROM src
),

domain_build AS (
    SELECT
        *,
        CASE
            WHEN base_store IN ('glbo','glcl','glcr','glgt','glhn','glmx','glmy','glpa','glpe','glph')
                THEN CONCAT('glamira.com.', REPLACE(base_store, 'gl', ''))

            WHEN base_store = 'glza'
                THEN 'glamira.co.za'

            ELSE CONCAT('glamira.', REPLACE(base_store, 'gl', ''))
        END AS domain

    FROM clean
),

currency_fix AS (
    SELECT
        d.*,

        COALESCE(cm.currency, d.raw_currency) AS final_currency

    FROM domain_build d
    LEFT JOIN currency_map cm
        ON d.domain = cm.domain
),

price_fix AS (
    SELECT *,
        CASE
            WHEN price < min_price THEN min_price
            WHEN price > max_price THEN max_price
            ELSE price
        END AS final_price
    FROM currency_fix
),

dedup AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY final_price DESC
            ) AS rn
        FROM price_fix
    )
    WHERE rn = 1
)

SELECT
    FARM_FINGERPRINT(CAST(product_id AS STRING)) AS product_key,
    product_id,
    name,
    category_id,
    product_type,
    sku,
    collection,

    final_price AS price,
    min_price,
    max_price,

    final_currency AS currency,
    store_code,

    ingested_at

FROM dedup