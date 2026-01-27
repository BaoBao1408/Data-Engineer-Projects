# (A) Baseline vs Optimized Summary Table
# Performance Comparison

## Query Runtime Comparison

| Query | Baseline (ms) | Optimized (ms) | Improvement |
|------|--------------|----------------|------------|
| Total revenue per month | ~7,200 | ~700 | ~10x |
| Filter order_item by product_id | ~1,270 | ~9 | ~140x |
| Orders by seller in October | ~500 | ~226 | ~2.2x |
| Products sold per seller | ~7,300 | ~5,300 | ~1.4x |

# (B) Execution Plan Changes (Quan trọng)
## Key Execution Plan Improvements

### Before Optimization
- Sequential Scan on full tables
- External merge sort spilling to disk
- No partition pruning
- High memory & disk I/O

### After Optimization
- Parallel Append on partitions
- Bitmap Index Scan on product_id
- HashAggregate replacing GroupAggregate
- Drastically reduced scanned rows

# (C) Concrete Evidence
## Evidence from EXPLAIN ANALYZE

- Rows scanned reduced from ~5.5M → ~4K
- Disk usage reduced from ~90MB → ~0
- Execution time reduced from seconds → milliseconds
- Partition pruning confirmed via Append node

# (D) Engineering Takeaways
## Lessons Learned

- Indexes alone are insufficient without proper partitioning
- Filtering on partition key is mandatory for large tables
- EXPLAIN ANALYZE is the only source of truth
- Performance tuning is iterative, not theoretical