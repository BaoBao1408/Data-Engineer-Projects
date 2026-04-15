{{ config(materialized='view') }}

SELECT
    -- =====================
    -- KEYS
    -- =====================
    event_id,
    session_id,

    user_id ,
    store_id ,
    product_id ,

    location_key,

    -- =====================
    -- METRICS
    -- =====================
    final_quantity AS quantity,
    price,

    -- =====================
    -- TIME
    -- =====================
    DATE(event_time) AS event_date,

    -- =====================
    -- METADATA
    -- =====================
    CURRENT_TIMESTAMP() AS ingested_at

FROM {{ ref('stg_user_event') }}

WHERE
    event_type = 'checkout_success'
    AND product_id IS NOT NULL