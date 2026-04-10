WITH source AS (

    SELECT *
    FROM {{ source('glamira_raw', 'dim_location') }}

),

-- remove header row
filtered AS (

    SELECT *
    FROM source
    WHERE string_field_0 != 'ip'   -- remove header row

),

renamed AS (

    SELECT
        SAFE_CAST(string_field_0 AS STRING) AS ip
        ,SAFE_CAST(string_field_1 AS STRING) AS country
        ,SAFE_CAST(string_field_2 AS STRING) AS region
        ,SAFE_CAST(string_field_3 AS STRING) AS city

    FROM filtered

),

cleaned AS (

    SELECT *
    FROM renamed
    WHERE ip IS NOT NULL

)

SELECT * FROM cleaned