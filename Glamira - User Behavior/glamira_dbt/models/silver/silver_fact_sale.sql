{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_fact_sale') }}
),

clean AS (
    SELECT
        event_id,
        session_id,

        user_id,
        store_id,
        product_id,
        location_key,

        SAFE_CAST(quantity AS INT64) AS quantity,
        SAFE_CAST(price AS FLOAT64) AS price,

        DATE(event_date) AS event_date,

        CURRENT_TIMESTAMP() AS ingested_at
    FROM src
),

mapped AS (
    SELECT
        s.event_id,
        s.session_id,

        -- ✅ giữ full data + map key
        COALESCE(c.customer_key, -1) AS customer_key,
        COALESCE(p.product_key, -1) AS product_key,
        COALESCE(st.store_key, -1) AS store_key,
        COALESCE(s.location_key, -1) AS location_key,

        s.quantity,
        s.price,
        s.event_date,
        s.ingested_at

    FROM clean s

    LEFT JOIN {{ ref('silver_dim_customer') }} c
        ON CAST(s.user_id AS STRING) = c.user_id

    LEFT JOIN {{ ref('silver_dim_product') }} p
        ON s.product_id = p.product_id

    LEFT JOIN {{ ref('silver_dim_store') }} st
        ON s.store_id = st.store_id
),

dedup AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY event_date DESC
            ) AS rn
        FROM mapped
    )
    WHERE rn = 1
)

SELECT
    event_id,
    session_id,
    customer_key,
    product_key,
    store_key,
    location_key,
    quantity,
    price,
    event_date,
    ingested_at
FROM dedup