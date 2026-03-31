# Project 04 – SQL Analytics

## Phase 01 – Optimization Techniques

---

## 1. Objective

Phase 01 focuses on **understanding query performance**, **analyzing execution plans**, and **identifying optimization opportunities** on large transactional datasets.

The goal is **not** to rewrite business logic, but to:

* Establish **baseline performance**
* Learn how PostgreSQL executes analytical queries
* Build a solid foundation for **partitioning and indexing** in Phase 02

---

## 2. Dataset Overview

The analytical workload is based on an e-commerce transactional model with the following fact tables:

* `orders` (~2.5–3 million rows)
* `order_item` (~7–10 million rows)

And supporting dimension tables:

* `seller`
* `product`
* `brand`
* `category`

Fact tables are intentionally large to simulate **real-world analytical pressure**.

---

## 3. Baseline Strategy

All queries in Phase 01 are executed against **non-partitioned tables**:

* `orders`
* `order_item`

Key principles:

* No indexes (except primary keys)
* No partitioning
* Queries reflect **business requirements only**

Each query is analyzed using:

```sql
EXPLAIN ANALYZE
```

Snapshots include:

* Execution time
* Scan strategy
* Join method
* Aggregation behavior
* Sorting behavior

---

## 4. Analytical Queries (Q01–Q08)

### Q01 – Total Revenue per Month

**Purpose:** Aggregate total revenue by month.

Key observations:

* Full sequential scan on `orders`
* GroupAggregate with disk-based external sort
* High I/O cost due to lack of partition pruning

---

### Q02 – Revenue by Seller per Month

**Purpose:** Analyze seller performance over time.

Key observations:

* Parallel sequential scan on `orders`
* Hash Join with `seller` dimension
* External merge sort due to large intermediate result set

---

### Q03 – Filter Order Items by Product

**Purpose:** Retrieve order items for a specific product or product range.

Key observations:

* Full table scan on `order_item`
* No index usage
* High execution time even for selective filters

---

### Q04 – Order with Highest Total Amount

**Purpose:** Identify the highest-value order.

Key observations:

* Full scan on `orders`
* Full sort required before applying LIMIT
* Inefficient without ordering optimizations

---

### Q05 – Products with Highest Quantity Sold

**Purpose:** Identify top-selling products.

Key observations:

* Heavy aggregation on `order_item`
* Large fact table dominates execution time
* Sorting required for ranking

---

### Q06 – Orders by Seller in October

**Purpose:** Count orders per seller in a specific month.

Key observations:

* Sequential scan with date filter
* No partition pruning available
* Moderate improvement from reduced row set

---

### Q07 – Revenue per Product per Month

**Purpose:** Analyze product-level revenue trends over time.

Key observations:

* Fact-to-fact joins (`orders` ↔ `order_item`)
* High-cardinality aggregation (product × month)
* One of the most expensive queries in the workload

---

### Q08 – Products Sold per Seller

**Purpose:** Measure seller performance by total quantity sold.

Key observations:

* Large fact-to-fact join
* Aggregation at seller level
* Partitioning alone expected to have limited benefit

---

## 5. Key Performance Patterns Observed

Across all baseline queries, the following patterns were consistently observed:

* Full sequential scans on large fact tables
* External merge sorts due to memory limits
* Hash joins dominating execution cost
* Dimension tables contributing negligible overhead

This confirms that **fact table scan volume** and **aggregation grain** are the primary performance bottlenecks.

---

## 6. Why No Query Rewrites in Phase 01?

Phase 01 intentionally avoids:

* Changing SELECT logic
* Removing dimension joins
* Introducing indexes or partitions

Rationale:

> Optimization should be applied at the **storage and execution level**, not by altering business logic.

This ensures fair and meaningful comparison in Phase 02.

---

## 7. Deliverables

Phase 01 produces the following artifacts:

```
phase_01_optimization_techniques/
├── baseline_queries/
│   ├── q01.sql
│   ├── q02.sql
│   └── ...
├── snapshots/
│   └── baseline/
│       ├── q01_explain.txt
│       ├── q02_explain.txt
│       └── ...
```

Each snapshot contains:

* Query execution plan
* Runtime statistics
* Observed bottlenecks

---

## 8. Summary & Transition to Phase 02

Phase 01 establishes a **clear performance baseline** and demonstrates that:

* Large fact tables dominate query cost
* Time-based analytics suffer without partitioning
* Indexes are critical for selective filters

These findings directly motivate Phase 02:

> **Applying monthly partitioning and indexing to improve analytical query performance.**

---

**Next Phase:**

➡️ **Phase 02 – Making Dynamic Reports (Functions & Procedures)**

Where performance-optimized tables will be used to build reusable, parameterized analytical reports.

## 🙌 Author
## 🙌 Name: Quoc Bao
## 🙌 Email: Baoquocnguyen1408@gmail.com