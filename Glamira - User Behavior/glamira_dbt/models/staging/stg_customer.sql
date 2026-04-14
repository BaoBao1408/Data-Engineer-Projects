{{ config(materialized='view') }}

SELECT
    -- session / user
    session_id,
    SAFE_CAST(user_id AS INT64) AS user_id,
    email_address,

    -- device
    device,
    user_agent,
    resolution,

    -- time
    event_time,
    local_time,

    -- event
    LOWER(event_type) AS event_type

FROM {{ source('glamira_raw', 'user_event') }}

WHERE 
    user_id IS NOT NULL
    AND email_address IS NOT NULL