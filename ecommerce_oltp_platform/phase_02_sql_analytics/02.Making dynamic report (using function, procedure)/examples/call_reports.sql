-- Monthly revenue
SELECT * FROM reporting.fn_monthly_revenue('2025-08-01', '2025-11-01');

-- Daily revenue with product filter
SELECT * FROM reporting.fn_daily_revenue(
    '2025-10-01',
    '2025-10-31',
    ARRAY[100,101,102]
);

-- Seller performance
SELECT * FROM reporting.fn_seller_performance(
    '2025-08-01',
    '2025-11-01',
    NULL,
    NULL
);
