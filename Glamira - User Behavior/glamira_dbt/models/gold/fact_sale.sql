WITH base AS (

    SELECT
        e.event_id
        ,e.user_id
        ,e.product_id
        ,e.store_code
        ,e.ip
        ,e.session_id
        ,e.event_time
        ,e.local_time
        ,e.current_url

    FROM {{ ref('stg_user_event') }} e

),

product_price AS (

    SELECT
        product_id
        ,price as price
    FROM {{ source('glamira_raw', 'dim_product') }}

)

SELECT
    b.event_id
    ,b.user_id
    ,b.product_id
    ,b.store_code
    ,b.ip
    ,b.session_id
    ,b.event_time

    ,p.price
    ,b.local_time
    ,b.current_url

FROM base b
LEFT JOIN product_price p
    ON b.product_id = p.product_id