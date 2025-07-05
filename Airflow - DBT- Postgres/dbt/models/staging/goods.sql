SELECT
    id,
    category_id,
    name,
    price,
    created_at,
    updated_at
FROM {{ source('staging', 'goods') }}
