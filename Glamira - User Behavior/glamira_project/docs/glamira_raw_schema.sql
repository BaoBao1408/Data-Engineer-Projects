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



