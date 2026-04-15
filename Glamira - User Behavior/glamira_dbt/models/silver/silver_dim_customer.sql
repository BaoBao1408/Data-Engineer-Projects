WITH clean AS (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        LOWER(TRIM(email_address)) AS email_address,
        event_time,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM {{ ref('stg_customer') }}
    WHERE user_id IS NOT NULL
),

final AS (
    SELECT
        FARM_FINGERPRINT(user_id) AS customer_key,
        user_id,
        email_address,
        event_time,
        ingested_at
    FROM clean
)

SELECT * FROM final