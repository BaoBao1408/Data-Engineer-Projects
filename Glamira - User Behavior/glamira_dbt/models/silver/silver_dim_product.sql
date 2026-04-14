{{ config(enabled=false) }}

SELECT DISTINCT
    CAST(product_id AS STRING) AS product_id
    ,name
    ,category_name
    ,product_type
    ,min_price
    ,max_price

FROM {{ source('glamira_raw', 'dim_product') }}