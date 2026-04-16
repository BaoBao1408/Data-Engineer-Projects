{{ config(materialized='table') }}

WITH fact AS (
    SELECT * FROM {{ ref('silver_fact_sale') }}
),

currency_rate AS (
    SELECT * FROM {{ ref('dim_currency_rate') }}
),


joined AS (
    SELECT
        f.event_id,
        f.session_id,

        f.customer_key,
        f.product_key,
        f.store_key,
        f.location_key,
        f.price,
        f.quantity,
        f.event_date,
        f.ingested_at,

        --  currency from store
        st.currency AS raw_currency,

        -- normalize currency
        CASE 
            WHEN st.currency IS NULL OR st.currency = '' THEN 'EUR'
            WHEN LOWER(st.currency) = 'unknown' THEN 'EUR'
            ELSE st.currency
        END AS currency_clean

    FROM fact f

    LEFT JOIN {{ ref('gold_dim_product') }} p
        ON f.product_key = p.product_key

    LEFT JOIN {{ ref('gold_dim_store') }} st
        ON f.store_key = st.store_key

    LEFT JOIN {{ ref('gold_dim_customer') }} c
        ON f.customer_key = c.customer_key
        AND c.is_current = TRUE

    LEFT JOIN {{ ref('gold_dim_location') }} l
        ON f.location_key = l.location_key
),

joined_rate AS (
    SELECT
        j.*,

        COALESCE(r.rate_to_eur, 1) AS rate_to_eur,
        j.price * COALESCE(r.rate_to_eur, 1) AS price_eur

    FROM joined j

    LEFT JOIN currency_rate r
        ON j.currency_clean = r.currency
),

with_time AS (
    SELECT
        *,
        CAST(FORMAT_TIMESTAMP('%Y%m%d%H', TIMESTAMP(event_date)) AS INT64) AS time_key
    FROM joined_rate
),

joined_time AS (
    SELECT
        wt.*,
        t.full_timestamp
    FROM with_time wt
    LEFT JOIN {{ ref('gold_dim_time') }} t
        ON wt.time_key = t.time_key
),

final AS (
    SELECT
        event_id,
        session_id,

        customer_key,
        product_key,
        store_key,
        location_key,

        CAST(price AS NUMERIC) AS price,
        quantity,

        price_eur * quantity AS revenue,

        event_date,
        time_key,

        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        ingested_at

    FROM joined_time
)

SELECT * FROM final