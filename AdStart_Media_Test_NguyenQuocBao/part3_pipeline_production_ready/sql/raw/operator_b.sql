-- sql/raw/operator_b.sql
-- SUB rows have rotate_id; REN/UNSUB rows have rotate_id = NULL (by design).
-- Attribution for REN: chain via msisdn → most recent SUB → rotate_id → campaign.

DELETE FROM raw_operator_b WHERE _loaded_date = :run_date;

INSERT INTO raw_operator_b
SELECT
    CAST(transaction_id     AS VARCHAR)      AS transaction_id,
    CAST(rotate_id          AS VARCHAR)      AS rotate_id,      -- NULL for REN/UNSUB
    CAST(msisdn             AS VARCHAR)      AS msisdn,
    UPPER(TRIM(transaction_type))            AS transaction_type,
    TRY_CAST(amount         AS DOUBLE)       AS amount,
    COALESCE(CAST(currency AS VARCHAR), 'GBP') AS currency,
    TRY_CAST(received_time  AS TIMESTAMPTZ)  AS created_at,    -- CSV col: received_time
    DATE :run_date                           AS _loaded_date
FROM read_csv_auto(:file_path, header=true, null_padding=true);
