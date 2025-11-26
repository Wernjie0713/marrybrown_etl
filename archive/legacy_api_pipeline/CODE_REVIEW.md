# Code Review: transform_api_to_facts.py & extract_fast_sample.py

**Date:** December 2025  
**Reviewer:** AI Code Review  
**Files Reviewed:**
- `api_etl/transform_api_to_facts.py`
- `api_etl/extract_fast_sample.py`

---

## 🔴 CRITICAL ISSUES

### 1. SQL Injection Vulnerability (extract_fast_sample.py:215-218)
**Severity:** CRITICAL  
**Location:** `get_location_keys_batch()` function

```python
# VULNERABLE CODE:
outlets_list = [name.replace("'", "''") for name in valid_outlets]
outlets_str = "','".join(outlets_list)
query = f"""
    SELECT LocationName, LocationKey FROM dim_locations 
    WHERE LocationName IN ('{outlets_str}')
"""
```

**Problem:** String interpolation in SQL queries is vulnerable to SQL injection, even with quote escaping.

**Fix:** Use parameterized queries:
```python
# SAFE CODE:
placeholders = ",".join([f":outlet_{i}" for i in range(len(valid_outlets))])
query = f"""
    SELECT LocationName, LocationKey FROM dim_locations 
    WHERE LocationName IN ({placeholders})
"""
params = {f"outlet_{i}": name for i, name in enumerate(valid_outlets)}
result = conn.execute(text(query), params).fetchall()
```

---

## 🟠 HIGH PRIORITY ISSUES

### 2. Incorrect Time Calculation (transform_api_to_facts.py:319)
**Severity:** HIGH  
**Location:** `transform_to_facts_optimized()` function

```python
chunk_time = time.time() - start_time  # This is TOTAL time, not per-chunk!
```

**Problem:** Reports total elapsed time as if it's per-chunk time, misleading performance metrics.

**Fix:**
```python
chunk_start = time.time()
# ... process chunk ...
chunk_time = time.time() - chunk_start
```

### 3. Inconsistent CostAmount Calculation
**Severity:** HIGH  
**Location:** Multiple functions

- Line 202 (optimized): `si.CostAmount * pa.allocation_percentage`
- Line 431 (legacy): `si.Cost * si.Quantity * pa.allocation_percentage`
- Line 613 (window): `si.Cost * si.Quantity * pa.allocation_percentage`

**Problem:** Different functions calculate CostAmount differently. If `staging_sales_items` has a `CostAmount` column, the optimized version is correct. Otherwise, it should match the legacy version.

**Recommendation:** Verify which approach is correct and standardize across all functions.

### 4. Error Handling in Parallel Writes (extract_fast_sample.py:767-769)
**Severity:** HIGH  
**Location:** `write_parallel()` function

```python
except Exception as e:
    print(f"  [ERROR] Failed to write {table_name}: {e}")
    results[table_name] = 0  # Silently fails!
```

**Problem:** Errors are swallowed, making debugging difficult and potentially causing data inconsistency.

**Fix:** At minimum, log the full traceback. Consider re-raising or using a more sophisticated error handling strategy:
```python
except Exception as e:
    print(f"  [ERROR] Failed to write {table_name}: {e}")
    import traceback
    traceback.print_exc()
    # Optionally: raise or use a callback mechanism
    results[table_name] = 0
```

### 5. Missing Error Handling in Cleanup (transform_api_to_facts.py:76-80)
**Severity:** MEDIUM  
**Location:** `cleanup_staging()` function

**Problem:** No error handling for cleanup operations. If cleanup fails, the entire transformation might appear successful but staging data accumulates.

**Fix:** Add try-except with logging:
```python
try:
    conn.execute(cutoff_sql, {"days": retention_days})
    print("  [STAGING] Retention cleanup complete.")
except Exception as e:
    print(f"  [WARNING] Cleanup failed (non-fatal): {e}")
    # Log but don't fail the entire transformation
```

---

## 🟡 MEDIUM PRIORITY ISSUES

### 6. Unused Imports
**Severity:** LOW  
**Location:** Both files

**extract_fast_sample.py:**
- `NullPool` (line 36) - imported but never used
- `Decimal`, `InvalidOperation` (line 41) - imported but never used
- `math` (line 42) - imported but never used

**transform_api_to_facts.py:**
- `DataQualityValidator` (line 21) - only used in commented code

**Fix:** Remove unused imports to improve code clarity.

### 7. Potential Division by Zero (extract_fast_sample.py:1013)
**Severity:** LOW  
**Location:** `extract_fast_sample()` function

```python
print(f"  Average API Call Time: {duration/call_count:.1f}s" if call_count > 0 else "")
```

**Status:** ✅ Already protected with conditional check. Good!

### 8. Inconsistent Connection Pooling
**Severity:** MEDIUM  
**Location:** Both files

- `transform_api_to_facts.py`: Creates engine without explicit pool settings (uses defaults)
- `extract_fast_sample.py`: Uses explicit pool settings (pool_size=5, max_overflow=10)

**Recommendation:** Standardize connection pooling configuration across both files for consistency.

### 9. Memory Concerns with Large Accumulation
**Severity:** MEDIUM  
**Location:** `extract_fast_sample.py:854-955`

**Problem:** `accumulated_sales` list can grow very large (up to `BATCH_ACCUMULATION_SIZE` records) in memory before flushing.

**Recommendation:** Consider using a generator or streaming approach for very large datasets, or add memory monitoring.

### 10. Missing Type Hints
**Severity:** LOW  
**Location:** Multiple functions

**Examples:**
- `get_warehouse_engine()` - no return type hint
- `cleanup_staging()` - has return type hint ✅
- `transform_to_facts_optimized()` - no return type hint
- `write_sales_batch()` - no return type hint

**Recommendation:** Add type hints for better code documentation and IDE support.

---

## 🟢 CODE QUALITY IMPROVEMENTS

### 11. Magic Numbers
**Location:** Multiple locations

- `chunk_size=10000` (transform_api_to_facts.py:83)
- `timeout=90` (extract_fast_sample.py:154)
- `pool_size=5`, `max_overflow=10` (extract_fast_sample.py:130-131)

**Recommendation:** Move to configuration constants or environment variables.

### 12. Duplicate Code
**Location:** Both files

Both files have nearly identical `get_warehouse_engine()` functions with slight differences:
- Different default driver versions (18 vs 17)
- Different pool configurations

**Recommendation:** Extract to a shared utility module.

### 13. Hardcoded Table Names
**Location:** Multiple locations

Table names like `'dbo.staging_sales'`, `'dbo.fact_sales_transactions'` are hardcoded throughout.

**Recommendation:** Consider using constants or configuration for table names to make refactoring easier.

### 14. Inconsistent Naming
**Location:** extract_fast_sample.py

- Function: `get_location_keys_batch()` (plural "keys")
- Function: `get_location_key_from_outlet()` (singular "key")

**Recommendation:** Standardize naming conventions.

### 15. Complex SQL Queries
**Location:** transform_api_to_facts.py:137-313

The MERGE query is very long (176 lines). While functional, it's difficult to maintain.

**Recommendation:** Consider breaking into smaller CTEs or using a query builder for better readability.

### 16. Missing Validation
**Location:** transform_api_to_facts.py:339-344

Data quality validation is commented out. This is noted in comments, but consider implementing a date-agnostic validation method.

---

## ✅ POSITIVE OBSERVATIONS

1. **Good Error Handling:** Retry logic in `perform_api_call()` is well-implemented with exponential backoff.
2. **Transaction Management:** Proper use of `engine.begin()` context managers for automatic rollback.
3. **Resume Capability:** Good implementation of checkpoint/resume functionality.
4. **Performance Optimizations:** Vectorized pandas operations, parallel writes, batch processing.
5. **Documentation:** Good docstrings explaining function purposes and optimizations.
6. **Chunked Processing:** Prevents memory issues with large datasets.
7. **BIT Field Handling:** Properly fixed with `CAST(MAX(CAST(IsFOC AS INT)) AS BIT)`.

---

## 📋 SUMMARY

### Critical Issues: 1
### High Priority: 4
### Medium Priority: 5
### Code Quality: 6

### Priority Actions:
1. **IMMEDIATE:** Fix SQL injection vulnerability in `get_location_keys_batch()`
2. **HIGH:** Fix time calculation bug in chunk processing
3. **HIGH:** Standardize CostAmount calculation across all functions
4. **HIGH:** Improve error handling in parallel writes
5. **MEDIUM:** Remove unused imports
6. **MEDIUM:** Standardize connection pooling configuration

---

## 🔧 RECOMMENDED REFACTORING

1. **Extract shared utilities:**
   - Create `utils/db_connection.py` for `get_warehouse_engine()`
   - Create `utils/sql_helpers.py` for common SQL operations

2. **Configuration management:**
   - Move all magic numbers to configuration file
   - Use environment variables with sensible defaults

3. **Error handling strategy:**
   - Implement consistent error handling pattern
   - Add structured logging instead of print statements
   - Consider using a logging framework (e.g., `logging` module)

4. **Testing:**
   - Add unit tests for critical functions
   - Add integration tests for ETL pipeline
   - Test error scenarios (network failures, DB errors, etc.)

---

**End of Code Review**

