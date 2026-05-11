-- sql/dimensions/dim_campaigns.sql
-- SCD Type 1: INSERT OR IGNORE keeps existing rows unchanged.
-- AWS: Glue job upsert into Redshift, or dbt snapshot for SCD Type 2.

INSERT OR IGNORE INTO dim_campaigns
SELECT
    id            AS campaign_id,
    operator,
    service_name,
    service_model,
    partner_id,
    status,
    created_at,
    now()         AS loaded_at
FROM raw_campaigns
WHERE id IS NOT NULL;
