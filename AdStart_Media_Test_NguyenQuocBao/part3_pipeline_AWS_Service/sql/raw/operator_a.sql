-- sql/raw/operator_a.sql
-- Idempotent load: delete run_date rows then insert.
-- Source column: received_time → staged as event_time (avoids DuckDB reserved-word collision)
-- event_code: 1=subscribe, 2=bill, 3=unsubscribe

DELETE FROM raw_operator_a WHERE _loaded_date = :run_date;

INSERT INTO raw_operator_a
SELECT
    CAST(transaction_id AS VARCHAR)          AS transaction_id,
    CAST(rotate_id      AS VARCHAR)          AS rotate_id,
    CAST(msisdn         AS VARCHAR)          AS msisdn,
    CAST(event_code     AS INTEGER)          AS event_code,
    UPPER(TRIM(CAST(status AS VARCHAR)))     AS status,
    TRY_CAST(amount     AS DOUBLE)           AS amount,
    COALESCE(CAST(currency AS VARCHAR), 'GBP') AS currency,
    TRY_CAST(received_time AS TIMESTAMPTZ)   AS event_time,   -- CSV col: received_time
    DATE :run_date                           AS _loaded_date
FROM read_csv_auto(:file_path, header=true, null_padding=true);
