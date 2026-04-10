SELECT
    event_id
    ,CAST(user_id AS STRING) AS user_id
    ,CAST(product_id AS STRING) AS product_id
    ,CAST(store_id AS STRING) AS store_code

    ,ip
    ,session_id
    ,device
    ,user_agent

    ,event_time
    ,local_time
    ,current_url

FROM {{ source('glamira_raw', 'fact_user_event') }}
WHERE product_id IS NOT NULL