# Phase 02 – SQL Analytics & Reporting

## Overview
Phase 02 focuses on **analytical SQL, performance optimization, and reporting-layer design** on top of large-scale OLTP data.

This phase simulates how a Data Engineer:
- Diagnoses slow analytical queries
- Applies physical optimizations (partitioning, indexing)
- Designs a reusable, BI-friendly reporting layer using SQL functions and procedures

The work is split into **two tightly connected parts**:
1. Optimization Techniques (query-level & storage-level)
2. Making Dynamic Reports (production-style SQL reporting layer)

---

## Part 1 – Optimization Techniques

### Objectives
- Understand how PostgreSQL executes analytical queries on large tables
- Identify performance bottlenecks using `EXPLAIN ANALYZE`
- Apply structural optimizations and measure real performance gains

### Scope
- Baseline vs optimized queries
- Execution plan analysis (cost, actual time, rows, memory, disk usage)
- Partitioning & indexing strategies
- Performance comparison using real metrics (ms, scanned rows, disk spill)

### Optimization Techniques Applied
- **Monthly partitioning** on large fact tables:
  - `orders_partitioned`
  - `order_item_partitioned`
- **Indexing**:
  - B-tree index on `order_item_partitioned(product_id)`
- **Query refactoring**:
  - Filter pushdown
  - Partition-aware predicates on `order_date`
  - Reduced unnecessary joins
  - Optimized aggregation patterns

### Query Scenarios Analyzed
1. Total revenue per month  
2. Orders filtered by seller and date  
3. Filter data in `order_item` by `product_id`  
4. Order with highest `total_amount`  
5. Products with highest quantity sold  
6. Orders by seller in October  
7. Revenue per product per month  
8. Products sold per seller  

Each scenario includes:
- Baseline query
- Optimized query
- `EXPLAIN ANALYZE` output
- Runtime comparison
- Technical explanation of improvements

### Key Learnings
- How partition pruning drastically reduces scanned data
- When PostgreSQL uses:
  - Seq Scan vs Bitmap Index Scan
  - HashAggregate vs GroupAggregate
- Impact of external merge sort and memory limits
- Benefits of parallel execution on large fact tables

---

## Part 2 – Making Dynamic Reporting Layer

### Objectives
- Build a **production-style SQL reporting layer**
- Support dynamic business filters
- Ensure queries remain performant on partitioned data
- Prepare SQL outputs for BI tools and orchestration (Airflow)

### Why Functions Instead of Views?
- Enable **parameterized filtering** (date range, product list, seller, brand, category)
- Avoid proliferation of static views
- Better reuse across BI tools and pipelines
- Explicit control over query behavior and performance

### Design Principles
- Select only required columns
- Push aggregations down to fact tables
- Join dimension tables **only when needed**
- Enforce filtering on `order_date` for partition pruning
- Avoid window functions in reporting layer
- Sorting delegated to BI layer when possible

### Implemented Reports
- **Monthly Revenue Report**
- **Daily Revenue Report** (with product list filter)
- **Seller Performance Report**
- **Top Products per Brand**
- **Orders Status Summary**

Each report supports:
- Date range filtering
- Optional dimension filters (seller, brand, category, product list)
- BI-ready output schema

### SQL Artifacts
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