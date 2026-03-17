# Phase 02.2 – Making Dynamic Reports (Notes)

## Design Decisions

### 1. Why SQL Functions instead of Views?
- Functions allow dynamic filtering (date range, product list, seller list)
- Better reuse across BI tools
- Avoid creating too many static views

### 2. Partition Awareness
- All reports enforce filtering on `order_date`
- Enables partition pruning on:
  - orders_partitioned
  - order_item_partitioned
- Prevents full table scan on large fact tables

### 3. Aggregation Strategy
- Aggregations are pushed down to fact tables
- Dimension joins only added when:
  - Required for output
  - Required for filtering

### 4. Performance Considerations
- `ANY(array)` used for list filtering → index friendly
- Avoid window functions in reporting layer
- Sorting delegated to BI layer where possible

### 5. Why Procedure Exists but Does Nothing (Yet)
- Prepared for future:
  - Materialized views
  - Incremental refresh
  - Airflow orchestration
- Keeps architecture extensible without premature optimization
