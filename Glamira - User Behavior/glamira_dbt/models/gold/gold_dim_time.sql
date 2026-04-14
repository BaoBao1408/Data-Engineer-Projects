{{ config(materialized='table') }}

WITH time_range AS (
    SELECT
        TIMESTAMP('2020-01-01 00:00:00') AS start_ts,
        TIMESTAMP('2030-12-31 23:00:00') AS end_ts
),

generated AS (
    SELECT
        ts AS full_timestamp
    FROM time_range,
    UNNEST(
        GENERATE_TIMESTAMP_ARRAY(start_ts, end_ts, INTERVAL 1 HOUR)
    ) AS ts
),

final AS (
    SELECT
        -- 🔑 surrogate key YYYYMMDDHH
        CAST(FORMAT_TIMESTAMP('%Y%m%d%H', full_timestamp) AS INT64) AS time_key,

        full_timestamp,

        DATE(full_timestamp) AS date,

        EXTRACT(YEAR FROM full_timestamp) AS year,
        EXTRACT(QUARTER FROM full_timestamp) AS quarter,
        EXTRACT(MONTH FROM full_timestamp) AS month,
        EXTRACT(DAY FROM full_timestamp) AS day,
        EXTRACT(HOUR FROM full_timestamp) AS hour,

        EXTRACT(DAYOFWEEK FROM full_timestamp) AS day_of_week,

        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM full_timestamp) IN (1,7) THEN TRUE
            ELSE FALSE
        END AS is_weekend

    FROM generated
)

SELECT * FROM final