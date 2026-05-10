-- sql/raw/operator_c.sql
-- DELIVERED = subscribe + charge in one event.
-- ~13% of tracking_codes are > 3 chars (operator SMS parser bug) → logged, not dropped.

DELETE FROM raw_operator_c WHERE _loaded_date = :run_date;

INSERT INTO raw_operator_c
SELECT
    CAST(message_id       AS VARCHAR)        AS message_id,
    CAST(tracking_code    AS VARCHAR)        AS tracking_code,
    CAST(msisdn           AS VARCHAR)        AS msisdn,
    UPPER(TRIM(delivery_status))             AS delivery_status,
    CAST(service_id       AS VARCHAR)        AS service_id,
    TRY_CAST(received_time AS TIMESTAMPTZ)   AS received_time,
    DATE :run_date                           AS _loaded_date
FROM read_csv_auto(:file_path, header=true, null_padding=true);
