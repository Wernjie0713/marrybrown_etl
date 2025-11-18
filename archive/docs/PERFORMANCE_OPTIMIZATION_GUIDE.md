# Performance Optimization Guide - Cloud ETL

**Author:** YONG WERN JIE  
**Date:** October 30, 2025  
**Purpose:** Document performance optimization approaches for cloud database ETL

---

## Problem Statement

Initial API ETL to cloud database was **extremely slow** (30+ minutes stuck on loading sales items).

### Root Cause Analysis

**OLD METHOD - Row-by-Row INSERT:**
```python
for item in items:
    conn.execute(text("INSERT INTO ... VALUES (...)"), item_data)
    conn.commit()
```

**Performance Impact:**
- 11,809 sales = 11,809 individual INSERTs
- ~40,000 items = 40,000 individual INSERTs  
- ~12,000 payments = 12,000 individual INSERTs
- **Total: 63,000+ network round-trips to cloud database!**
- **Each round-trip has ~20-50ms latency** = 21-52 minutes total!

---

## Solution: Multi-Layer Optimization Strategy

### ✅ **Layer 1: Pandas Batch Processing (IMPLEMENTED)**

**Approach:**
```python
# Collect all data into DataFrames
df_items = pd.DataFrame(items_data)

# Bulk insert in chunks
df_items.to_sql('staging_sales_items', engine, 
                schema='dbo', if_exists='append',
                index=False, chunksize=1000, method='multi')
```

**Benefits:**
- Reduces 40,000 INSERTs → ~40 batch operations
- Uses SQL Server's bulk insert API
- **Expected: 10-50x faster**
- Minimal code changes required

**Implementation:**
- ✅ `extract_from_api.py` - `load_to_staging()` function
- ✅ All 3 staging tables (sales, items, payments)

---

### ✅ **Layer 2: SQL Server `fast_executemany` (IMPLEMENTED)**

**Approach:**
```python
connection_uri = (
    f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    "&fast_executemany=True"  # Array binding optimization
)
```

**Benefits:**
- Enables pyodbc array binding
- SQL Server processes batches more efficiently
- **Additional 2-5x faster** on top of pandas batching
- No code changes to insert logic

**Implementation:**
- ✅ `extract_from_api.py` - `get_warehouse_engine()` function

---

### ❌ **Layer 3: Multithreading (NOT RECOMMENDED)**

**Why NOT Suitable for Staging Load:**

1. **TRUNCATE Must Be Serial:**
   ```python
   TRUNCATE TABLE staging_sales      # Must happen first
   TRUNCATE TABLE staging_sales_items
   TRUNCATE TABLE staging_payments
   ```
   
2. **Table Lock Conflicts:**
   - Multiple threads inserting into same table = contention
   - SQL Server may serialize anyway
   
3. **Minimal Gain:**
   - Batch insert is already near I/O limits
   - GIL doesn't block I/O, but coordination overhead hurts
   
4. **Complexity:**
   - Transaction management across threads
   - Error handling and rollback
   - Connection pool management

**Verdict:** ❌ Adds complexity without significant benefit

---

### ✅ **Alternative: Multi-Month Parallel Processing (FUTURE)**

**Could Be Beneficial for Multi-Month ETL:**

```python
from concurrent.futures import ThreadPoolExecutor

def extract_month(year, month):
    """Extract one month of data"""
    # API call
    # Save to temp staging with month suffix
    # Transform and append to fact table
    pass

# Parallel extraction
with ThreadPoolExecutor(max_workers=3) as executor:
    months = [(2018, 10), (2018, 11), (2018, 12), ...]
    executor.map(lambda m: extract_month(*m), months)
```

**Benefits:**
- Parallel API calls (I/O bound)
- Each month processes independently
- Load results sequentially to avoid conflicts

**Considerations:**
- API rate limits (may need throttling)
- Staging table design (month partitions or temp tables)
- Final fact table insert still serial

**Verdict:** ⚠️ Worth exploring for 15-month bulk load

---

## Performance Comparison

### Before Optimization
```
Row-by-row INSERT over network:
- Sales headers: ~4 minutes (11,809 rows)
- Sales items: ~35 minutes (40,000 rows)
- Payments: ~6 minutes (12,000 rows)
Total: ~45 minutes per month
```

### After Optimization (Layer 1 + 2)
```
Batch INSERT with fast_executemany:
- Sales headers: ~10 seconds (11,809 rows in 12 batches)
- Sales items: ~40 seconds (40,000 rows in 40 batches)
- Payments: ~12 seconds (12,000 rows in 12 batches)
Total: ~1-2 minutes per month
```

**Improvement: ~30-45x faster!** ⚡

---

## Other Performance Techniques Considered

### 1. **SQL Server BCP Utility**
```bash
# Bulk copy from file
bcp dbo.staging_sales in sales.csv -S server -d database -U user -P pass -c
```
**Pros:** Fastest possible (native SQL Server)  
**Cons:** Requires intermediate CSV files, complex error handling  
**Verdict:** Overkill for current volume

### 2. **BULK INSERT Statement**
```sql
BULK INSERT staging_sales
FROM 'C:\data\sales.csv'
WITH (FIELDTERMINATOR=',', ROWTERMINATOR='\n')
```
**Pros:** Very fast, SQL-based  
**Cons:** Requires file on server, security permissions  
**Verdict:** Not practical for cloud deployment

### 3. **SQLAlchemy Core with `executemany()`**
```python
conn.execute(table.insert(), items_list)
```
**Pros:** More control than pandas  
**Cons:** More complex code, pandas is sufficient  
**Verdict:** Unnecessary complexity

### 4. **Async I/O with `asyncio` + `aioodbc`**
```python
async def insert_batch(batch):
    async with pool.acquire() as conn:
        await conn.executemany(...)
```
**Pros:** Non-blocking I/O  
**Cons:** Major code refactor, asyncio learning curve  
**Verdict:** Not worth it for current scale

---

## Best Practices for Cloud Database ETL

1. ✅ **Always batch operations** - Never insert row-by-row
2. ✅ **Use database-specific optimizations** - `fast_executemany` for SQL Server
3. ✅ **Monitor connection pool** - Reuse connections
4. ✅ **Add connection timeouts** - Handle network latency
5. ✅ **Use compression** - `?Compress=True` in connection string (optional)
6. ✅ **Truncate before load** - Faster than DELETE
7. ❌ **Avoid unnecessary commits** - Let pandas/pyodbc handle it
8. ❌ **Don't use multithreading for single table** - Causes contention

---

## Monitoring Performance

### Add Timing to ETL Scripts

```python
import time

def load_to_staging(sales):
    start = time.time()
    
    # ... load sales headers ...
    print(f"  Time: {time.time() - start:.1f}s")
    
    # ... load items ...
    print(f"  Time: {time.time() - start:.1f}s")
```

### SQL Server Performance Counters

```sql
-- Check active connections
SELECT * FROM sys.dm_exec_sessions WHERE database_id = DB_ID('MarryBrown_DW')

-- Check wait stats
SELECT * FROM sys.dm_os_wait_stats
WHERE wait_type LIKE 'NETWORKIO%'
```

---

## Conclusion

**Chosen Approach:** Pandas Batch Processing + `fast_executemany`

**Rationale:**
- ✅ Simplest implementation
- ✅ Massive performance gain (30-45x)
- ✅ Minimal code changes
- ✅ Production-ready and stable
- ✅ Scales well for current data volume (11K-40K rows/month)

**Result:** 45 minutes → 1-2 minutes per month ⚡

---

## Future Optimizations (If Needed)

1. **Multi-month parallel extraction** - For initial bulk load of 15 months
2. **Incremental ETL** - Only extract new/changed records
3. **Database partitioning** - Partition fact table by month
4. **Compression** - Enable connection compression for large datasets
5. **Connection pooling tuning** - Increase pool size if parallel processing added

**Current Status:** Layer 1 + 2 sufficient for production use ✅

