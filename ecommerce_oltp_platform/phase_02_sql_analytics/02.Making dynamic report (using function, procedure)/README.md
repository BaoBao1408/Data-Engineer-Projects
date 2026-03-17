# Phase 02 – SQL Analytics & Reporting

## Overview
Phase 02 focuses on **analytical SQL**, performance optimization, and building a **reporting-ready SQL layer** on top of a large OLTP-style dataset (~millions of rows).

This phase is divided into two sub-phases:
1. Optimization Techniques (baseline vs optimized queries)
2. Making Dynamic Reports using SQL Functions & Procedures

---

## 01. Optimization Techniques

### Objectives
- Analyze query performance on large datasets
- Identify bottlenecks using `EXPLAIN ANALYZE`
- Apply optimization techniques:
  - Table partitioning
  - Indexing
  - Query rewriting
- Compare baseline vs optimized execution plans

### Key Topics
- Sequential Scan vs Index Scan
- HashAggregate vs GroupAggregate
- External merge sort & memory usage
- Parallel execution
- Partition pruning

### Implemented Optimizations
- Monthly partitioning on:
  - `orders_partitioned`
  - `order_item_partitioned`
- Index on:
  - `order_item_partitioned(product_id)`
- Query refactoring to:
  - Push down filters
  - Reduce scanned rows
  - Improve aggregation strategy

### Query Scenarios
1. Total revenue per month  
2. Orders filtered by seller and date  
3. Filter `order_item` by `product_id`  
4. Order with highest total amount  
5. Products with highest quantity sold  
6. Orders by seller in October  
7. Revenue per product per month  
8. Products sold per seller  

Each query includes:
- Baseline execution plan
- Optimized execution plan
- Runtime comparison
- Analysis & explanation

---

## 02. Making Dynamic Reports (Functions & Procedures)

### Objectives
- Build reusable, parameterized SQL reports
- Support dynamic filters (date range, product list, seller, brand, category)
- Ensure partition-aware and index-friendly queries
- Prepare SQL layer for BI tools and orchestration (Airflow)

### Design Principles
- SQL **functions instead of views** for flexibility
- Fact-table–first aggregation
- Dimension joins only when required
- Avoid over-materialization
- BI-ready outputs (clean schema & column naming)

### Implemented Reports
- Monthly Revenue Report
- Daily Revenue Report (with product filter)
- Seller Performance Report
- Top Products per Brand
- Orders Status Summary

### Architecture
```text
02_Making_dynamic_report/
├── ddl/
│   └── reporting_schema.sql
├── functions/
│   ├── fn_monthly_revenue.sql
│   ├── fn_daily_revenue.sql
│   ├── fn_seller_performance.sql
│   ├── fn_top_products_per_brand.sql
│   └── fn_order_status_summary.sql
├── procedures/
│   └── sp_refresh_reports.sql
├── examples/
│   └── call_reports.sql
├── notes.md
└── README.md

## 🙌 Author
## 🙌 Name: Quoc Bao
## 🙌 Email: Baoquocnguyen1408@gmail.com