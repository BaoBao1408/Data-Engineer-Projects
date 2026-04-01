-- =====================================================
-- DATASET: glamira_raw
-- PURPOSE: FULL DATA MODEL (PRODUCT + USER EVENT + IP)
-- =====================================================


-- =====================================================
-- 1. EXTERNAL TABLE: PRODUCT (JSONL FROM GCS)
-- =====================================================

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

  -- handle inconsistent schema
  min_price STRING,
  max_price STRING,

  react_data JSON
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://glamira-data-lake-qb/raw/products/*.jsonl']
);


-- =====================================================
-- 2. DIM_PRODUCT
-- =====================================================

CREATE OR REPLACE TABLE `glamira_raw.dim_product` AS
SELECT DISTINCT
  product_id,
  name,
  category AS category_name,
  JSON_VALUE(react_data, '$.product_type') AS product_type,
  collection,
  gender
FROM `glamira_raw.tmp_ext_products`
WHERE product_id IS NOT NULL;


-- =====================================================
-- 3. DIM_SKU
-- =====================================================

CREATE OR REPLACE TABLE `glamira_raw.dim_sku` AS
SELECT DISTINCT
  sku,
  product_id,
  url,
  JSON_VALUE(react_data, '$.store_code') AS store_code
FROM `glamira_raw.tmp_ext_products`
WHERE sku IS NOT NULL;


-- =====================================================
-- 4. FACT_PRODUCT_PRICE
-- =====================================================

CREATE OR REPLACE TABLE `glamira_raw.fact_product_price`
PARTITION BY load_date
CLUSTER BY product_id, sku AS
SELECT
  sku,
  product_id,

  SAFE_CAST(price AS FLOAT64) AS base_price,
  SAFE_CAST(price AS FLOAT64) AS final_price,

  SAFE_CAST(
    COALESCE(
      JSON_VALUE(react_data, '$.min_price'),
      min_price
    ) AS FLOAT64
  ) AS min_price,

  SAFE_CAST(
    COALESCE(
      JSON_VALUE(react_data, '$.max_price'),
      max_price
    ) AS FLOAT64
  ) AS max_price,

  CURRENT_DATE() AS load_date
FROM `glamira_raw.tmp_ext_products`
WHERE sku IS NOT NULL;


-- =====================================================
-- 5. FACT_PRODUCT_OPTION
-- =====================================================

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


-- =====================================================
-- 6. EXTERNAL TABLE: IP LOCATION (CSV)
-- =====================================================

CREATE OR REPLACE EXTERNAL TABLE `glamira_raw.ext_ip_location`
OPTIONS (
  format = 'CSV',
  uris = ['gs://glamira-data-lake-qb/raw/ip_location/raw_ip_location.csv'],
  skip_leading_rows = 1
);


-- =====================================================
-- 7. DIM_IP_LOCATION
-- =====================================================

CREATE OR REPLACE TABLE `glamira_raw.ip_location`
CLUSTER BY ip_from, ip_to AS
SELECT
  SAFE_CAST(ip_from AS INT64) AS ip_from,
  SAFE_CAST(ip_to AS INT64) AS ip_to,

  country,
  region,
  city
FROM `glamira_raw.ext_ip_location`
WHERE ip_from IS NOT NULL;


-- =====================================================
-- 8. EXTERNAL TABLE: USER EVENT (JSONL)
-- =====================================================

CREATE OR REPLACE EXTERNAL TABLE `glamira_raw.tmp_user_event`
OPTIONS (
  format = 'JSON',
  uris = ['gs://glamira-data-lake-qb/raw/glamira/part_*.jsonl']
);


-- =====================================================
-- 9. FACT_USER_EVENT
-- =====================================================

CREATE OR REPLACE TABLE `glamira_raw.fact_user_event`
PARTITION BY DATE(event_time)
CLUSTER BY event_type, product_id AS
SELECT *
FROM `glamira_raw.tmp_user_event`;


-- =====================================================
-- 10. OPTIONAL: ENRICH USER EVENT WITH IP LOCATION
-- =====================================================

-- Example usage:
-- SELECT
--   e.*,
--   l.country,
--   l.city
-- FROM `glamira_raw.fact_user_event` e
-- JOIN `glamira_raw.ip_location` l
-- ON e.ip_num BETWEEN l.ip_from AND l.ip_to;


-- =====================================================
-- 11. DATA QUALITY CHECK
-- =====================================================

-- product count
SELECT COUNT(*) AS product_count FROM `glamira_raw.dim_product`;

-- sku count
SELECT COUNT(*) AS sku_count FROM `glamira_raw.dim_sku`;

-- option count
SELECT COUNT(*) AS option_count FROM `glamira_raw.fact_product_option`;

-- event count
SELECT COUNT(*) AS event_count FROM `glamira_raw.fact_user_event`;