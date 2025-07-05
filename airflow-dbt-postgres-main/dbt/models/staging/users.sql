SELECT
    id,
    name,
    email,
    phone,
    created_at,
    updated_at
FROM {{ source('staging', 'users') }}
