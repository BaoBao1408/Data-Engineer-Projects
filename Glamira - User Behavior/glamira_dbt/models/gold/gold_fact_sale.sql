{{ config(materialized='table') }}

WITH fact AS (
    SELECT * FROM {{ ref('silver_fact_sale') }}
),

joined AS (
    SELECT
        f.event_id,
        f.session_id,

        f.customer_key,
        f.product_key,
        f.store_key,
        f.location_key,

        f.price,
        f.quantity,
        f.event_date,
        f.ingested_at

    FROM fact f

    LEFT JOIN {{ ref('gold_dim_product') }} p
        ON f.product_key = p.product_key

    LEFT JOIN {{ ref('gold_dim_store') }} st
        ON f.store_key = st.store_key

    LEFT JOIN {{ ref('gold_dim_customer') }} c
        ON f.customer_key = c.customer_key
        AND c.is_current = TRUE

    LEFT JOIN {{ ref('gold_dim_location') }} l
        ON f.location_key = l.location_key
),

with_time AS (
    SELECT
        *,
        CAST(FORMAT_TIMESTAMP('%Y%m%d%H', TIMESTAMP(event_date)) AS INT64) AS time_key
    FROM joined
),

final AS (
    SELECT
        event_id,
        session_id,

        customer_key,
        product_key,
        store_key,
        location_key,

        CAST(price AS NUMERIC) AS price,
        quantity,

        event_date,
        time_key,

        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        ingested_at

    FROM with_time
)

SELECT * FROM final