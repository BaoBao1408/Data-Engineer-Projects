-- Monthly revenue
SELECT * 
FROM reporting.fn_monthly_revenue(
    '2025-08-01',
    '2025-11-01'
);

-- Daily revenue with product filter
SELECT * 
FROM reporting.fn_daily_revenue(
    '2025-10-01',
    '2025-10-31',
    ARRAY[100,101,102]
);

-- Seller performance (no category / brand filter)
SELECT * 
FROM reporting.fn_seller_performance(
    '2025-08-01',
    '2025-11-01',
    NULL,   -- category_id
    NULL    -- brand_id
);

-- ==============================
-- NEW EXAMPLES
-- ==============================

-- Top products per brand
-- (Optional filter by seller list)
SELECT * 
FROM reporting.fn_top_products_per_brand(
    '2025-08-01',
    '2025-11-01',
    NULL        -- seller_ids (e.g. ARRAY[1,2,3])
);

-- Orders status summary
-- (Optional filter by seller list)
SELECT * 
FROM reporting.fn_order_status_summary(
    '2025-08-01',
    '2025-11-01',
    NULL        -- seller_ids (e.g. ARRAY[10,20])
);
