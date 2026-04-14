{{ config(enabled=false) }}
SELECT DISTINCT
    user_id
    ,session_id
    ,ip
    ,device
    ,user_agent

FROM {{ ref('stg_user_event') }}
WHERE user_id IS NOT NULL