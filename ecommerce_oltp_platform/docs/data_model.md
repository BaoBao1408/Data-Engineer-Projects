# (A) OLTP Model (Phase 01)
# Data Model

## OLTP Schema (Source System)

### Design Principles
- Normalized (3NF)
- Write-optimized
- Strong referential integrity

### Core Tables
- orders (fact-like, transactional)
- order_item (line-level fact)
- product, seller, brand, category (dimensions)
- promotion & promotion_product (N:N relationship)

### Key Relationships
- One order → many order_items
- One seller → many products → many orders
- Promotions applied at query time (no denormalization)

# (B) Analytical Perspective (Phase 02)   
## Analytical Modeling Strategy

### Fact Tables
- orders_partitioned
- order_item_partitioned

### Dimensions
- product
- seller
- brand
- category

### Partitioning Strategy
- Partition key: order_date (monthly)
- Reason:
  - Time-based analytics
  - Partition pruning
  - Incremental processing

### Grain Definition
- orders: 1 row = 1 order
- order_item: 1 row = 1 product per order

# (C) Future Warehouse Model (Phase 03 – Planned)
## Cloud Warehouse Model (Planned)

### Fact Tables
- fct_sales
  - order_date
  - product_id
  - seller_id
  - quantity
  - revenue

### Dimensions
- dim_product
- dim_seller
- dim_brand
- dim_category

### Modeling Approach
- Star schema
- ELT with dbt
- Partitioned by date, clustered by business keys
