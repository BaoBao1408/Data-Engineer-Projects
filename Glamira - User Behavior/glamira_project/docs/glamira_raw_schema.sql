CREATE OR REPLACE EXTERNAL TABLE `glamira_raw.tmp_ext_products`
(
  _id STRING,
  product_id STRING,
  url STRING,
  name STRING,
  sku STRING,
  price FLOAT64,
  category STRING,
  gender STRING,
  collection STRING,

  min_price STRING,   
  max_price STRING,   

  react_data JSON
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://glamira-data-lake-qb/raw/products/*.jsonl']
);


CREATE OR REPLACE TABLE `glamira_raw.dim_product` AS
SELECT
  product_id,

  ANY_VALUE(name) AS name,
  ANY_VALUE(sku) AS sku,
  ANY_VALUE(category) AS category_name,

  MAX(
    CASE
      WHEN JSON_VALUE(react_data, '$.product_type') NOT IN ('-1', '--_select_--', '')
      THEN JSON_VALUE(react_data, '$.product_type')
    END
  ) AS product_type,

  MAX(
  CASE
    WHEN JSON_VALUE(react_data, '$.collection') NOT IN ('', 'null')
    THEN JSON_VALUE(react_data, '$.collection')
  END
) AS collection,
  MAX(JSON_VALUE(react_data, '$.store_code')) AS store_code,

  ANY_VALUE(gender) AS gender,

  MAX(price) AS price,

  MAX(SAFE_CAST(JSON_VALUE(react_data, '$.min_price') AS FLOAT64)) AS min_price,
  MAX(SAFE_CAST(JSON_VALUE(react_data, '$.max_price') AS FLOAT64)) AS max_price

FROM `glamira_raw.tmp_ext_products`
WHERE product_id IS NOT NULL
GROUP BY product_id;


CREATE OR REPLACE TABLE `glamira_raw.dim_sku` AS
SELECT DISTINCT
  sku,
  product_id,
  url,
  JSON_VALUE(react_data, '$.store_code') AS store_code
FROM `glamira_raw.tmp_ext_products`
WHERE sku IS NOT NULL;


CREATE OR REPLACE TABLE `glamira_raw.fact_product_price`
PARTITION BY load_date
CLUSTER BY product_id, sku AS
SELECT
  sku,
  product_id,
  SAFE_CAST(price AS FLOAT64) AS base_price,
  SAFE_CAST(price AS FLOAT64) AS final_price,
  SAFE_CAST(JSON_VALUE(react_data, '$.min_price') AS FLOAT64) AS min_price,
  SAFE_CAST(JSON_VALUE(react_data, '$.max_price') AS FLOAT64) AS max_price,
  CURRENT_DATE() AS load_date
FROM `glamira_raw.tmp_ext_products`
WHERE sku IS NOT NULL;


CREATE OR REPLACE TABLE `glamira_raw.fact_product_option`
CLUSTER BY product_id, sku AS
SELECT
  p.product_id,
  p.sku,
  JSON_VALUE(opt, '$.option_id') AS option_id,
  JSON_VALUE(opt, '$.part_type') AS option_type,
  JSON_VALUE(val, '$.sku') AS option_value,
  SAFE_CAST(JSON_VALUE(val, '$.price') AS FLOAT64) AS option_price
FROM `glamira_raw.tmp_ext_products` p,
UNNEST(JSON_QUERY_ARRAY(p.react_data, '$.options')) AS opt,
UNNEST(JSON_QUERY_ARRAY(opt, '$.values')) AS val
WHERE p.sku IS NOT NULL;


CREATE OR REPLACE EXTERNAL TABLE `glamira_raw.raw_ip_location`
OPTIONS (
  format = 'CSV',
  uris = ['gs://glamira-data-lake-qb/raw/ip_location/raw_ip_location.csv'],
  skip_leading_rows = 1
);


LOAD DATA INTO glamira_raw.fact_user_event
FROM FILES (
  format = 'JSON',
  uris = ['gs://glamira-data-lake-qb/raw/glamira/part_*.jsonl']
);


CREATE OR REPLACE TABLE glamira_raw.fact_user_event
PARTITION BY DATE(event_time)
CLUSTER BY event_type, product_id AS
SELECT *
FROM glamira_raw.tmp_user_event;

---v2
CREATE OR REPLACE EXTERNAL TABLE glamira_raw.ext_user_event
(
  event_id STRING,
  event_time STRING,
  event_type STRING,

  user_id STRING,
  session_id STRING,
  email_address STRING,

  product_id STRING,
  quantity STRING,
  price STRING,
  currency STRING,  -- define

  ip STRING,
  user_agent STRING,
  device STRING,
  resolution STRING,

  current_url STRING,
  referrer_url STRING,

  store_id STRING,

  utm_source STRING,
  utm_medium STRING,
  recommendation STRING,

  local_time STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://glamira-data-lake-qb/raw/glamira_upgrade_2/*.jsonl'],
  ignore_unknown_values = TRUE   -- KEY FIX
);

CREATE OR REPLACE TABLE glamira_raw.user_event
PARTITION BY DATE(event_time)
CLUSTER BY event_type, store_id AS

SELECT
  -- IDs
  event_id,

  -- TIME
  SAFE_CAST(event_time AS TIMESTAMP) AS event_time,
  SAFE_CAST(local_time AS TIMESTAMP) AS local_time,

  -- EVENT
  event_type,

  -- USER
  SAFE_CAST(user_id AS INT64) AS user_id,
  session_id,
  email_address,

  -- PRODUCT
  SAFE_CAST(product_id AS INT64) AS product_id,
  SAFE_CAST(quantity AS INT64) AS quantity,

  -- 💰 PRICE CLEAN 
  SAFE_CAST(
    REPLACE(
      REPLACE(
        REPLACE(price, '.', ''),   -- remove thousand separator
      ',', '.'),                   -- convert decimal
    '€', '')                      -- remove currency
  AS FLOAT64) AS price,

  currency,

  -- DEVICE
  ip,
  user_agent,
  device,
  resolution,

  -- NAVIGATION
  current_url,
  referrer_url,

  -- BUSINESS
  SAFE_CAST(store_id AS INT64) AS store_id,

  -- TRACKING
  utm_source,
  utm_medium,
  SAFE_CAST(recommendation AS BOOL) AS recommendation

FROM glamira_raw.ext_user_event
WHERE event_time IS NOT NULL


--- sales_dashboard
CREATE OR REPLACE VIEW glamira_gold.sales_dashboard AS
SELECT
    f.event_id,
    f.event_date,
    f.time_key,

    f.price,
    f.quantity,

    f.revenue,

    -- TIME DIM

    t.year,
    t.month,
    t.day,
    t.day_of_week,
    t.is_weekend,

    -- =====================
    -- PRODUCT DIM
    -- =====================

    p.product_key,
    p.name AS product_name,
    p.category_id,
    p.product_type,
    p.collection,

    -- =====================
    -- STORE DIM
    -- =====================
    st.store_key,
    st.store_code,
    st.country AS store_country,
    st.region,
    st.currency,
    st.language,

    -- =====================
    -- CUSTOMER DIM
    -- =====================
    c.customer_key,
    c.email_address,

    -- =====================
    -- LOCATION DIM
    -- =====================
    l.location_key,
    l.country AS location_country,
    l.region AS location_region,
    l.city

FROM glamira_gold.gold_fact_sale f

LEFT JOIN glamira_gold.gold_dim_product p
    ON f.product_key = p.product_key

LEFT JOIN (
    SELECT *
    FROM glamira_gold.gold_dim_store
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_key 
        ORDER BY updated_at DESC
    ) = 1
) st
ON f.store_key = st.store_key

LEFT JOIN glamira_gold.gold_dim_customer c
    ON f.customer_key = c.customer_key
    AND c.is_current = TRUE

LEFT JOIN glamira_gold.gold_dim_location l
    ON f.location_key = l.location_key

LEFT JOIN glamira_gold.gold_dim_time t
    ON f.time_key = t.time_key