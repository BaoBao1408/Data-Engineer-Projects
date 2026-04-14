{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_fact_sale') }}
),

clean AS (
    SELECT
        event_id,
        session_id,

        --  keys
        SAFE_CAST(customer_key AS INT64) AS customer_key,
        SAFE_CAST(store_key AS INT64) AS store_key,
        SAFE_CAST(product_key AS INT64) AS product_key,
        SAFE_CAST(location_key AS INT64) AS location_key,

        -- measure
        SAFE_CAST(quantity AS INT64) AS quantity,
        SAFE_CAST(price AS FLOAT64) AS price,

        -- time
        DATE(event_date) AS event_date,

        CURRENT_TIMESTAMP() AS ingested_at

    FROM src
),

filtered AS (
    SELECT *
    FROM clean
    WHERE
        product_key IS NOT NULL
        AND store_key IS NOT NULL
        AND quantity IS NOT NULL
        AND price IS NOT NULL
),

dedup AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY event_date DESC
            ) AS rn
        FROM filtered
    )
    WHERE rn = 1
)

SELECT
    event_id,
    session_id,
    customer_key,
    store_key,
    product_key,
    location_key,
    quantity,
    price,
    event_date,
    ingested_at

FROM dedup