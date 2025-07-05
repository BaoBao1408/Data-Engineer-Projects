SELECT
    id,
    order_id,
    goods_id,
    qty,
    created_at,
    updated_at
FROM {{ source('staging', 'order_item') }}
