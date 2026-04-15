{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('silver_fact_sale') }}
),

with_time AS (
    SELECT
        *,
        CAST(FORMAT_TIMESTAMP('%Y%m%d%H', TIMESTAMP(event_date)) AS INT64) AS time_key
    FROM src
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