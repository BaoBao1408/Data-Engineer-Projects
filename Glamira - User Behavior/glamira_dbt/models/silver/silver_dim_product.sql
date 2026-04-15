WITH clean AS (
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

        SPLIT(store_code, '_')[OFFSET(0)] AS base_store,

        CURRENT_TIMESTAMP() AS ingested_at

    FROM {{ ref('stg_product') }}
),

domain_build AS (
    SELECT *,
        CONCAT('glamira.', REPLACE(base_store, 'gl', '')) AS domain
    FROM clean
),

currency_fix AS (
    SELECT
        d.*,
        COALESCE(cm.currency, d.raw_currency) AS final_currency
    FROM domain_build d
    LEFT JOIN {{ ref('domain_currency') }} cm
        ON d.domain = cm.domain
),

dedup AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id, store_code
                ORDER BY ingested_at DESC
            ) AS rn
        FROM currency_fix
    )
    WHERE rn = 1
)

SELECT
    product_id AS product_key,
    product_id,
    name,
    category_id,
    product_type,
    sku,
    collection,
    price,
    min_price,
    max_price,
    final_currency AS currency,
    store_code,
    ingested_at
FROM dedup