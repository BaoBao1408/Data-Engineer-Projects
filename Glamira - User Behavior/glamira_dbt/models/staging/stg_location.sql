{{ config(materialized='view') }}

WITH raw AS (
    SELECT *
    FROM {{ source('glamira_raw', 'dim_location') }}
),

clean AS (
    SELECT
        string_field_0 AS ip
        ,string_field_1 AS country
        ,string_field_2 AS region
        ,string_field_3 AS city
    FROM raw
    WHERE string_field_0 != 'ip'  -- remove header row
),

final AS (
    SELECT
        -- FIX: dùng alias c
        FARM_FINGERPRINT(
            CONCAT(
                COALESCE(c.country, ''),
                '|',
                COALESCE(c.region, ''),
                '|',
                COALESCE(c.city, '')
            )
        ) AS location_key

        ,c.ip
        ,c.country
        ,c.region
        ,c.city

    FROM clean c
    WHERE c.country IS NOT NULL
)

SELECT * FROM final