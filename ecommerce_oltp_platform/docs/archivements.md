# Project Achievements – E-commerce OLTP Platform

## 1. Large-scale OLTP Data Generation
- Generated ~3.8M orders and ~13M order_items with realistic business constraints
- Implemented checkpoint & resume mechanism for long-running jobs
- Ensured strong referential integrity across 8+ related tables
- Simulated real OLTP performance degradation (WAL, indexes, I/O pressure)

## 2. SQL Performance Optimization at Scale
- Analyzed query performance using EXPLAIN ANALYZE with real metrics
- Identified bottlenecks:
  - Sequential scans on large fact tables
  - External merge sorts spilling to disk
- Applied optimizations:
  - Monthly partitioning on orders and order_item
  - Targeted indexing on product_id
- Achieved 5–10x runtime improvements on analytical queries

## 3. Partition-aware Analytical Design
- Enforced order_date filtering to enable partition pruning
- Reduced scanned rows from millions to thousands in filtered queries
- Validated improvements through execution plans, not assumptions

## 4. Dynamic Reporting Layer
- Designed reusable SQL functions instead of static views
- Supported parameterized filters:
  - Date range
  - Product list
  - Seller / brand / category
- Produced BI-ready outputs with minimal post-processing

## 5. Cloud-ready Architecture Design
- Designed end-to-end pipeline:
  PostgreSQL → GCS → BigQuery → dbt → BI
- Defined responsibilities of each layer:
  - Storage
  - Transformation
  - Orchestration
- Prepared project for Airflow & dbt integration (Phase 03)
