{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_store') }}   
),

store_mapping AS (
    SELECT *
    FROM {{ ref('store_mapping') }}
),

clean AS (
    SELECT
        s.store_code,

        -- 🎯 normalize store_code
        SPLIT(s.store_code, '_')[OFFSET(0)] AS base_store_code,

        LOWER(TRIM(s.region)) AS region,
        LOWER(TRIM(s.country)) AS country,
        LOWER(TRIM(s.language)) AS language,

        s.base_store,
        s.currency AS raw_currency,

        CURRENT_TIMESTAMP() AS ingested_at

    FROM src s
),

enriched AS (
    SELECT
        c.store_code,

        -- ✅ join đúng key
        sm.store_id,

        c.region,
        c.country,
        c.language,

        c.base_store,
        c.raw_currency,

        c.ingested_at

    FROM clean c
    LEFT JOIN store_mapping sm
        ON c.base_store_code = sm.store_code
),

final AS (
    SELECT
        FARM_FINGERPRINT(store_code) AS store_key,

        store_id,
        store_code,

        raw_currency AS currency,

        region,
        language,
        country,
        base_store,

        ingested_at

    FROM enriched
)

SELECT * FROM final