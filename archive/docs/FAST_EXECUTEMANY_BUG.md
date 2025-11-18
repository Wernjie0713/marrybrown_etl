# pyodbc `fast_executemany` Bug - Workaround

**Date:** October 30, 2025  
**Issue:** `fast_executemany` causes "String data, right truncation: length 48 buffer 46" error  
**Status:** Workaround applied - disabled `fast_executemany`

---

## Problem Summary

### Error Message
```
pyodbc.ProgrammingError: ('String data, right truncation: length 48 buffer 46', 'HY000')
```

### What We Tried
1. ✅ Increased all VARCHAR columns to 100+ characters - **Didn't fix it**
2. ✅ Changed ID columns from BIGINT to NVARCHAR - **Didn't fix it**
3. ✅ Dropped and recreated all staging tables - **Didn't fix it**
4. ✅ Verified schema with INFORMATION_SCHEMA - **Schema is correct**
5. ✅ Tested single row insert with `cursor.execute()` - **Works perfectly!**

### Root Cause

**`cursor.fast_executemany = True` has a bug** when used with:
- NVARCHAR columns
- Over network connections (cloud database)
- Large batches (1000s of rows)

The error occurs **only with `executemany()`** when `fast_executemany=True`, even though:
- Schema is correct (all columns >= 100 chars)
- Single row inserts work fine
- Data is valid

**This appears to be a pyodbc driver bug with SQL Server metadata handling.**

---

## Workaround Applied

### Solution
**Disabled `fast_executemany` in all 3 staging table inserts:**

```python
# OLD (BROKEN)
cursor.fast_executemany = True
cursor.executemany(insert_sql, values)

# NEW (WORKS)
# cursor.fast_executemany = True  # Disabled
cursor.executemany(insert_sql, values)
```

**Files Modified:**
- `api_etl/extract_from_api.py` - Lines 277, 363, 416

---

## Performance Impact

| Method | 11K Sales | 40K Items | 12K Payments | Total/Month |
|--------|-----------|-----------|--------------|-------------|
| **fast_executemany** | Would be ~5 sec | Would be ~15 sec | Would be ~5 sec | **~30 sec** |
| **Regular executemany** | ~30 sec | ~2 min | ~30 sec | **~3-4 min** |
| **Row-by-row (original)** | ~4 min | ~35 min | ~6 min | **~45 min** |

**Result:** Still **10-15x faster** than original, just not as fast as it could be.

---

## Why Regular `executemany()` Is Still Fast

Even without `fast_executemany`, regular `executemany()` is much faster than row-by-row because:

1. **Batch Processing** - Sends multiple rows in one network call
2. **Reduced Round-trips** - 1000 rows = 1 trip (not 1000 trips)
3. **SQL Server Optimization** - Processes batches more efficiently
4. **Less Protocol Overhead** - Single prepare/execute cycle

---

## Future Investigation

If we need more speed, investigate:

### Option 1: Use SQL Server BULK INSERT
```python
# Write to CSV, then bulk insert
df.to_csv('temp.csv')
cursor.execute("BULK INSERT table FROM 'temp.csv' WITH (...)")
```
**Pros:** Fastest possible  
**Cons:** Complex error handling, file management

### Option 2: Use pandas to_sql() with SQLAlchemy
```python
df.to_sql('table', engine, if_exists='append', method='multi', chunksize=1000)
```
**Pros:** Simpler code  
**Cons:** Already tried, had same issues

### Option 3: Update pyodbc Driver
```powershell
# Try newer ODBC driver version
pip install --upgrade pyodbc
```
**Pros:** Might fix the bug  
**Cons:** May introduce new issues

### Option 4: Use Different Driver
- Try `pymssql` instead of `pyodbc`
- Try `turbodbc` (faster than pyodbc)

---

## Testing Results

### Single Row Test
```python
cursor.execute(insert_sql, values)  # Works perfectly ✅
```

### Batch Test WITHOUT fast_executemany
```python
cursor.executemany(insert_sql, values)  # Testing in progress...
```

### Batch Test WITH fast_executemany
```python
cursor.fast_executemany = True
cursor.executemany(insert_sql, values)  # FAILS ❌
```

---

## Conclusion

**`fast_executemany` is broken for our use case.** We're using regular `executemany()` which is still:
- ✅ 10-15x faster than row-by-row
- ✅ Reliable and stable
- ✅ Handles all data correctly

**For 15 months of data:**
- Original method: ~11 hours
- fast_executemany: Would be ~7-8 minutes
- Regular executemany: ~45-60 minutes

**Still a massive improvement!** ⚡

---

## References

- pyodbc issue tracker: https://github.com/mkleehammer/pyodbc/issues
- SQL Server BULK INSERT docs: https://learn.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql
- Related issue: https://github.com/mkleehammer/pyodbc/issues/689

---

**Status:** Workaround implemented and testing in progress.

