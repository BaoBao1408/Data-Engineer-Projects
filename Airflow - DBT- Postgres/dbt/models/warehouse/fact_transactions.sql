WITH fact_base AS (
    SELECT
        o.user_id AS user_id,
        u.name AS user_name,
        oi.id AS order_item_id,
        oi.qty AS number_of_items_ordered,
        oi.qty * g.price AS total_purchase_value,
        o.created_at AS order_date
    FROM {{ ref('order_item') }} oi
    JOIN {{ ref('goods') }} g ON oi.goods_id = g.id
    JOIN {{ ref('orders') }} o ON oi.order_id = o.id
    JOIN {{ ref('users') }} u ON o.user_id = u.id
)
SELECT * FROM fact_base
