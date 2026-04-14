{# {{ config(materialized='view') }}

SELECT
    *,
    FARM_FINGERPRINT(
        CONCAT(
            COALESCE(country, ''),
            '|',
            COALESCE(region, ''),
            '|',
            COALESCE(city, '')
        )
    ) AS location_key
FROM {{ source('glamira_raw', 'user_event') }}
WHERE user_id IS NOT NULL

SELECT DISTINCT
  ip AS ip_address,
  user_agent,
  resolution,
  user_id_db AS user_id,
  device_id,
  store_id,
  order_id,
  email_address,
  SAFE_CAST(cart_products.product_id AS INT) AS product_id,
  cart_products.amount AS order_qty,
  DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', local_time)) AS order_local_time,
  time_stamp AS order_timestamp
FROM {{ source('glamira_raw', 'user_event') }}
WHERE collection = 'checkout_success' #}

{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('glamira_raw', 'user_event') }}
),

clean AS (
    SELECT
        -- =====================
        -- ID
        -- =====================
        event_id,

        -- =====================
        -- TIME
        -- =====================
        TIMESTAMP(event_time) AS event_time,
        TIMESTAMP(local_time) AS local_time,

        -- =====================
        -- EVENT
        -- =====================
        LOWER(event_type) AS event_type,

        -- =====================
        -- USER
        -- =====================
        SAFE_CAST(user_id AS INT64) AS user_id,
        session_id,
        email_address,

        -- =====================
        -- PRODUCT
        -- =====================
        SAFE_CAST(product_id AS INT64) AS product_id,
        SAFE_CAST(quantity AS INT64) AS quantity,

        -- =====================
        --  PRICE (ADD)
        -- =====================
        SAFE_CAST(
            CASE
                WHEN REGEXP_CONTAINS(CAST(price AS STRING), r',')
                THEN REPLACE(REPLACE(CAST(price AS STRING), '.', ''), ',', '.')
                ELSE CAST(price AS STRING)
            END
        AS FLOAT64) AS price,

        -- =====================
        -- BUSINESS
        -- =====================
        SAFE_CAST(store_id AS INT64) AS store_id,

        -- =====================
        -- DEVICE
        -- =====================
        ip,
        user_agent,
        device,
        resolution,

        -- =====================
        -- NAVIGATION
        -- =====================
        current_url,
        referrer_url,

        -- =====================
        -- TRACKING
        -- =====================
        utm_source,
        utm_medium,
        recommendation

    FROM source
    WHERE event_id IS NOT NULL
),

-- =====================
-- 🌍 LOCATION JOIN
-- =====================
location AS (
    SELECT
        ip,
        FARM_FINGERPRINT(
            CONCAT(
                COALESCE(country, ''),
                '|',
                COALESCE(region, ''),
                '|',
                COALESCE(city, '')
            )
        ) AS location_key,
        country,
        region,
        city
    FROM {{ ref('stg_location') }}
),

final AS (
    SELECT
        c.*,

        -- fallback quantity
        COALESCE(c.quantity, 1) AS final_quantity,

        -- location enrich
        l.location_key,
        l.country,
        l.region,
        l.city

    FROM clean c
    LEFT JOIN location l
    ON c.ip = l.ip
)

SELECT * FROM final