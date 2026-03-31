SELECT
    id,
    name,
    created_at,
    updated_at
FROM {{ source('staging', 'categories') }}
