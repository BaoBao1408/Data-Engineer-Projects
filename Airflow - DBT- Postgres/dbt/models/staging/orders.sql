SELECT
    id,
    user_id,
    is_refund,
    created_at,
    updated_at
FROM {{ source('staging', 'orders') }}
