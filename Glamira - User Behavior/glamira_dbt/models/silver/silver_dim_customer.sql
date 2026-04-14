{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

clean AS (
    SELECT
        CAST(user_id AS STRING) AS user_id,

        LOWER(TRIM(email_address)) AS email_address,

        event_time,

        CURRENT_TIMESTAMP() AS ingested_at

    FROM src
    WHERE user_id IS NOT NULL
),

dedup AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY user_idd
                ORDER BY event_time DESC
            ) AS rn
        FROM clean
    )
    WHERE rn = 1
),

final AS (
    SELECT
        FARM_FINGERPRINT(user_id) AS customer_key,

        user_id,
        email_address,

        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        ingested_at

    FROM dedup
)

SELECT * FROM final