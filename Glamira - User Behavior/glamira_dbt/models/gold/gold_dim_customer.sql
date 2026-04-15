{{ config(materialized='table') }}

WITH src AS (
    SELECT * FROM {{ ref('silver_dim_customer') }}
),

clean AS (
    SELECT
        CAST(customer_key AS INT64) AS customer_key,
        user_id,
        LOWER(email_address) AS email_address,
        ingested_at
    FROM src
),

scd AS (
    SELECT
        *,
        LAG(email_address) OVER (
            PARTITION BY user_id
            ORDER BY ingested_at
        ) AS prev_email
    FROM clean
),

change_flag AS (
    SELECT
        *,
        CASE
            WHEN prev_email IS NULL OR prev_email != email_address THEN 1
            ELSE 0
        END AS is_change
    FROM scd
),

versioned AS (
    SELECT
        *,
        SUM(is_change) OVER (
            PARTITION BY user_id
            ORDER BY ingested_at
        ) AS version
    FROM change_flag
),

base AS (
    SELECT
        customer_key,
        user_id,
        email_address,
        version,
        MIN(ingested_at) AS valid_from
    FROM versioned
    GROUP BY customer_key, user_id, email_address, version
),

final AS (
    SELECT
        customer_key,
        user_id,
        email_address,

        valid_from,

        LEAD(valid_from) OVER (
            PARTITION BY user_id
            ORDER BY valid_from
        ) AS valid_to,

        CASE
            WHEN LEAD(valid_from) OVER (
                PARTITION BY user_id
                ORDER BY valid_from
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM base
),

unknown AS (
    SELECT
        -1 AS customer_key,
        CAST(NULL AS STRING) AS user_id,
        'unknown' AS email_address,

        TIMESTAMP('1900-01-01') AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
)

SELECT * FROM final
UNION ALL
SELECT * FROM unknown