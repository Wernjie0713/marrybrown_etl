# Work Log - 3rd December

## Summary

Today's focus was on consolidating and refactoring the ETL replication architecture. After yesterday's successful pivot to direct streaming for large date-based tables, we analyzed the codebase to eliminate redundancy and improve clarity by renaming scripts and adding streaming capabilities to reference table replication.

---

## 1. Script Architecture Analysis & Decision

### Issue

- Had two separate replication scripts with overlapping functionality:
  - `scripts/export_and_load_replica.py` (1425 lines) - Disk-based (Parquet) approach for reference tables
  - `scripts/replicate_monthly_parallel.py` (437 lines) - Direct streaming for large date-based tables
- `replicate_monthly_parallel.py` imports helper functions FROM `export_and_load_replica.py`, creating a dependency
- Unclear which script to use for which purpose

### Analysis

- **export_and_load_replica.py:**
  - Approach: Source DB → Parquet file → Target DB (double disk I/O)
  - Works well: Small/medium reference tables (no date columns)
  - Fails: Large tables (connection timeouts, memory issues)
  - Contains: All shared helper functions (`prepare_data_for_sql`, `build_row_tuple`, etc.)
- **replicate_monthly_parallel.py:**
  - Approach: Source DB → Memory chunks → Target DB (direct streaming)
  - Works well: Large date-based tables (proven 25% faster)
  - Limitation: Only works for tables in `DATE_FILTER_COLUMNS`
  - Dependency: Imports critical functions from `export_and_load_replica.py`

### Decision

- **Chose Option 1:** Keep both scripts but refactor and rename
- Add direct streaming to `export_and_load_replica.py` for reference tables
- Rename both scripts to clearly indicate their purpose
- Eliminate disk I/O bottleneck for ALL tables

---

## 2. Script Refactoring & Renaming

### Changes Made

#### Script 1: `export_and_load_replica.py` → `replicate_reference_tables.py`

- **Added:** `stream_full_table_direct()` function for direct streaming of reference tables
- **New default:** `--full-table` mode now uses streaming (no Parquet)
- **Optional Parquet:** Available via `--full-table-mode parquet` flag
- **Kept:** All helper functions intact (other scripts depend on them)
- **Result:** Reference tables now also benefit from direct streaming performance

#### Script 2: `replicate_monthly_parallel.py` → `replicate_monthly_parallel_streaming.py`

- **Updated:** Import statements to use `replicate_reference_tables`
- **Refreshed:** Usage text and documentation
- **Kept:** All existing functionality unchanged

#### Backward Compatibility

- Created shim files at old script names:
  - `scripts/export_and_load_replica.py` (forwards to new name)
  - `scripts/replicate_monthly_parallel.py` (forwards to new name)
- Ensures existing workflows/cron jobs continue working

#### Updated Callers

- `scripts/debug_numeric_overflow.py`
- `scripts/debug_datetime_range.py`
- `scripts/debug_single_row_insert.py`
- `scripts/find_string_overflows.py`
- `scripts/run_replica_etl.py`

---

## 3. Documentation Updates

### Files Updated

- **docs/ETL.md:** Updated export/load examples to use new script names; documented streaming-first workflow
- **docs/INFRA.md:** Refreshed directory descriptions and script references
- **docs/HISTORY.md:** Added December 2025 entry documenting the streaming shift and renames
- **docs/README.md:** Adjusted schema file descriptions to reference new scripts

### New Documentation

- **docs/COMMANDS.md:** Created concise command reference covering:
  - Streaming full-table runs (default and Parquet mode)
  - Date-filtered loads
  - Monthly parallel streaming with resume
  - T-0/T-1 orchestration

---

## 4. Git Commit & Push

### Commit Details

- **Hash:** `0793c3e`
- **Files changed:** 12 files
- **Insertions:** +243 lines
- **Deletions:** -53 lines
- **Branch:** `main` → `origin/main`

### Commit Message

```
Refactor ETL scripts: rename and add direct streaming for all tables

- Renamed export_and_load_replica.py → replicate_reference_tables.py
- Renamed replicate_monthly_parallel.py → replicate_monthly_parallel_streaming.py
- Added stream_full_table_direct() for reference tables (no Parquet, direct streaming)
- Default --full-table mode now uses streaming; Parquet optional via --full-table-mode parquet
- Updated all imports in debug scripts and run_replica_etl.py
- Added backward-compatible shims for old script names
- Updated docs: ETL.md, INFRA.md, HISTORY.md, README.md
- Added docs/COMMANDS.md for quick command reference

Performance: Eliminates disk I/O bottleneck for both reference and large tables
```

---

## Files Modified/Created

### Renamed

- `scripts/export_and_load_replica.py` → `scripts/replicate_reference_tables.py`
- `scripts/replicate_monthly_parallel.py` → `scripts/replicate_monthly_parallel_streaming.py`

### Created

- `docs/COMMANDS.md` - Quick command reference
- `scripts/export_and_load_replica.py` - Backward-compatible shim
- `scripts/replicate_monthly_parallel.py` - Backward-compatible shim

### Modified

- `scripts/debug_datetime_range.py`
- `scripts/debug_numeric_overflow.py`
- `scripts/debug_single_row_insert.py`
- `scripts/find_string_overflows.py`
- `scripts/run_replica_etl.py`
- `docs/ETL.md`
- `docs/INFRA.md`
- `docs/HISTORY.md`
- `docs/README.md`

---

## 5. SQL Insert Performance Optimization (Phase 1)

### Context

After completing the script refactoring and establishing direct streaming as the baseline approach, we focused on SQL-level optimizations to further improve performance before considering library changes (Polars, turbodbc).

### Optimizations Implemented

#### 1. TABLOCK Hint for Reduced Lock Contention

- **Change:** Added `WITH (TABLOCK)` hint to INSERT statements
- **Purpose:** Enables table-level locking instead of row-level locking
- **Benefit:** Reduces lock contention between parallel workers
- **Expected Impact:** Allows using 4-5 workers instead of being limited to 2-3 due to deadlocks

#### 2. Dynamic Index Management

- **Disable Before Load:** Nonclustered indexes are disabled before each month's data load
- **Rebuild After Load:** Indexes are rebuilt after successful load completion
- **Graceful Fallback:** If index operations fail, script logs warnings and continues without crashing
- **Cleanup Handling:** If errors occur mid-load, index rebuild is attempted in cleanup phase
- **Benefit:** Bulk inserts are significantly faster without active indexes to maintain

#### 3. Optimized Batch and Commit Sizes

**New Defaults:**

- `chunk_size`: 50,000 rows (previously 10,000)
- `commit_interval`: 500,000 rows (previously 100,000)

**CLI Arguments:**

- `--chunk-size`: Override chunk/batch size
- `--commit-interval`: Override commit interval

**Rationale:**

- Larger batches reduce network round-trips
- Less frequent commits reduce transaction overhead
- Values tuned for typical sales table volume (~1-2M rows/month)

### Implementation Details

**New Helper Functions:**

- `list_nonclustered_indexes()` - Query table indexes from SQL Server
- `disable_nonclustered_indexes()` - Disable all nonclustered indexes
- `rebuild_nonclustered_indexes()` - Rebuild disabled indexes

**Modified Function:**

- `stream_month_to_target()` - Now handles index management per month

**Scope:**

- Index operations are per-month, not per-table (each worker handles its own month's indexes)
- Each thread maintains independent source and target connections

### Testing Command

```bash
# Recommended: 2 workers for optimal balance (avoids deadlocks)
python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --max-workers 2
```

**Optional Tuning:**

```bash
# Override batch sizes if needed
python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --max-workers 2 \
  --chunk-size 75000 \
  --commit-interval 750000
```

**Worker Count Guidelines:**

- `--max-workers 2`: **Recommended** - Best balance, minimal deadlocks
- `--max-workers 3`: May work but occasional deadlocks possible
- `--max-workers 4+`: High deadlock rate, not recommended despite TABLOCK optimization

### Expected Performance Improvement

- **TABLOCK + Index Management:** 20-30% faster inserts
- **Optimized Batch Sizes:** Reverted to 10k/100k (proven baseline)
- **Total Expected:** 20-30% improvement over baseline direct streaming

**Note:** Initial testing with larger batches (50k/500k) resulted in 45% performance regression due to wide table characteristics. Smaller batches are optimal for tables with many columns.

### Next Phase

After measuring these SQL-level improvements, Phase 2 will evaluate library swaps:

- Polars (replace Pandas)
- turbodbc (replace PyODBC)

### Real-World Testing Results

**Test Run:** 2024-01-01 to 2024-03-31 (3 months)

**Findings:**

- **4 workers:** Encountered deadlocks on chunk 0 of month 2024-02
  - Error: `Transaction was deadlocked on lock resources (1205)`
  - Script successfully retried (as designed), but high retry rate
- **Optimal worker count:** 2 workers
  - Minimal deadlocks
  - Balance between speed and stability
  - Previous successful runs confirmed this

**Conclusion:**
Even with TABLOCK and index management optimizations, concurrent writes to the same table create lock contention. The automatic retry mechanism works, but it's more efficient to avoid deadlocks entirely by using 2 workers.

**Documentation Updated:**

- `Logbook/3-12 work.md` - Worker count guidelines added
- `docs/COMMANDS.md` - Examples updated to use `--max-workers 2`

### Batch Size Regression & Revert

**Test Run 2:** 2024-01-01 to 2024-03-31 with larger batches (chunk_size=50k, commit_interval=500k)

**Performance Results:**

- **2 months in 35-40 minutes** = ~17.5-20 min/month
- **Baseline (10k/100k batches):** ~11 min/month
- **Regression:** 45% SLOWER instead of faster ❌

**Root Cause Analysis:**
Wide tables like `APP_4_SALES` (184 columns) create very large chunks:

- 50k rows × 184 columns = ~20MB per chunk
- Network or SQL Server memory bottlenecks with large chunks
- Smaller batches (10k = ~4MB) are more efficient for wide tables

**Actions Taken:**

1. **Reverted defaults:**
   - `chunk_size`: 50,000 → **10,000** rows
   - `commit_interval`: 500,000 → **100,000** rows
2. **Added timing instrumentation:**
   - Measure DELETE, DISABLE_IDX, INSERT, REBUILD_IDX per month
   - Log format: `[TIMING] APP_4_SALES 2024-01: DELETE 2.3s | DISABLE_IDX 0.5s | INSERT 4m32s | REBUILD_IDX 45s | TOTAL 5m20s`
   - Enables data-driven performance analysis

**CLI Arguments Retained:**

- `--chunk-size` and `--commit-interval` still available for experimentation
- Use proven defaults (10k/100k) unless testing specific scenarios

**Lesson Learned:**
Larger batches ≠ always faster. Batch size sweet spot depends on table width, network speed, and SQL Server capacity. Always measure before and after.

---

## 6. Polars Migration (Phase 2 Performance Optimization)

### Context

After reverting batch sizes to proven defaults, moved to Phase 2: Replace Pandas with Polars for native Rust performance.

### Implementation Approach

**Initial Attempt: Hybrid (Polars + Pandas)**

- Read data with Polars
- Convert to Pandas for `prepare_data_for_sql()`
- **Problem:** Conversion overhead negated Polars benefits

**Final Implementation: Full Native Polars**

- Eliminated ALL Pandas operations from the hot path
- Created `prepare_data_for_sql_polars()` - New Polars-native function
- Core pipeline: `Database → cursor.fetchmany() → Polars DataFrame → Polars prep → SQL Insert`

### Technical Details

**New Polars-Native Function:**

```python
prepare_data_for_sql_polars(pl_df: pl.DataFrame, schema_entry: dict) -> pl.DataFrame
```

**Functionality:**

- Datetime/Date parsing with SQL Server bounds checking
- Placeholder date conversion (`0001-01-01` → NULL)
- NaN/NaT sanitization
- Numpy scalar coercion to native Python types
- Uses Polars `.map_elements()` for transformations

**Hot Path Flow:**

1. `cursor.fetchmany(chunk_size)` - Fetch raw rows from source
2. `pl.DataFrame(rows, schema=columns)` - Create Polars DataFrame
3. `prepare_data_for_sql_polars(pl_chunk, schema_entry)` - Polars-native prep
4. `chunk_pl.select(columns).rows()` - Convert to Python tuples via Polars
5. `cursor.executemany(insert_sql, batch_data)` - Insert to SQL

**Zero Pandas Conversions:** No `.to_pandas()` or `pl.from_pandas()` in the processing loop

### Dependencies Installed

```bash
pip install polars
# Version: polars-1.35.2, polars-runtime-32-1.35.2
```

### Code Changes

**Modified:** `scripts/replicate_monthly_parallel_streaming.py`

- Added: `import polars as pl`
- Added: `prepare_data_for_sql_polars()` function (151 lines of Polars-native logic)
- Modified: `stream_month_to_target()` - Replaced Pandas chunking with cursor-based Polars
- Removed: All pandas imports and operations

**Preserved:**

- ✅ All timing instrumentation
- ✅ Delete-before-insert idempotency
- ✅ Index disable/rebuild logic
- ✅ TABLOCK hint
- ✅ Retry mechanisms
- ✅ Checkpoint system
- ✅ 2 workers default
- ✅ 10k/100k batch defaults

### Expected Performance Improvement

**Conservative:** 20-30% faster than Pandas baseline  
**Optimistic:** 40-50% faster (Rust-native operations)

**Factors:**

- Eliminated Python DataFrame overhead
- Rust-based type conversions (much faster than numpy/pandas)
- Native Polars memory management
- No conversion bottleneck

### Next Testing

Will measure timing logs to confirm improvement:

```
[TIMING] APP_4_SALES 2024-01: DELETE Xs | DISABLE_IDX Xs | INSERT Xm | REBUILD_IDX Xs | TOTAL Xm
```

Compare INSERT time against previous Pandas runs to quantify Polars benefit.

---

## 7. Polars Type Inference Debugging & Explicit Schema Implementation

### Problem Discovered

After implementing the full Polars migration, encountered **type inference errors** during data loading:

```
could not append value: 0 of type: i64 to the builder
make sure that all rows have the same schema or consider increasing `infer_schema_length`
```

**Root Cause:**

Polars was **guessing data types** from the first 100 rows (default `infer_schema_length=100`). When later rows had different types in the same column (e.g., empty strings `''` vs integers `0`), Polars failed with type mismatch errors.

**The Pandas Difference:**

- **Pandas:** Lenient - uses `object` dtype for mixed types, no errors
- **Polars:** Strict - enforces type consistency for performance, errors on type mismatches

### Decision: Use Raw Schema, Stop Guessing

Instead of letting Polars infer types from data samples, **use exact SQL Server types** from the source database schema:

```
Source DB Schema → Explicit Polars Schema → DataFrame Creation
(No inference, no guessing)
```

### Implementation

**Step 1: SQL → Polars Type Mapping**

Created a mapping function in `stream_month_to_target`:

```python
def map_sql_type_to_polars(sql_type: str) -> pl.DataType:
    t = (sql_type or "").lower()
    if t in ("int", "bigint", "smallint", "tinyint"):
        return pl.Int64
    if t in ("decimal", "numeric", "float", "real", "money"):
        return pl.Float64
    if t in ("date",):
        return pl.Date
    if t in ("datetime", "datetime2", "smalldatetime"):
        return pl.Datetime
    if t in ("bit",):
        return pl.Boolean
    if t in ("timestamp", "binary", "varbinary"):
        return pl.Binary
    return pl.Utf8  # default string
```

**Step 2: Build Explicit Schema from Source**

```python
polars_schema_base = {
    col["name"]: map_sql_type_to_polars(col.get("type", ""))
    for col in schema_entry["columns"]
}

# At runtime, match fetched columns
polars_schema = {col: polars_schema_base.get(col, pl.Utf8) for col in fetched_columns}
```

**Step 3: Use Explicit Schema in DataFrame Creation**

```python
pl.DataFrame(rows, schema=polars_schema, orient="row")
```

### Secondary Issue: Datetime Return Type Mismatch

After fixing the schema, encountered a new error:

```
expected output type 'Object', got 'Datetime('μs')'
```

**Cause:**

- Explicit schema typed datetime columns as `pl.Datetime`
- `prepare_data_for_sql_polars` used `.map_elements(..., return_dtype=pl.Object)` expecting Python objects
- Type mismatch between DataFrame column type and map function return type

**Fix:**

Updated `prepare_data_for_sql_polars` to use explicit return types matching the schema:

```python
pl.col(col_name).map_elements(
    lambda v: _convert_datetime_value(v, is_date),
    return_dtype=pl.Date if is_date else pl.Datetime,  # ← Explicit, not Object
).alias(col_name)
```

### Result

✅ **No more type inference errors**  
✅ **Schema-driven replication** - types come from source database, not guessed from data samples  
✅ **Type-safe casting** - Polars converts data to match declared schema (e.g., `0` in VARCHAR → `"0"`)  
✅ **True 1:1 replication** - source schema structure → target schema structure

### Regression: Polars return_dtype failures on mixed data (APP_4_SALES Apr–Jun)

- After removing pandas and forcing full Polars, repeated errors: `expected output type 'Date' got 'Datetime(µs)'` and `... got 'Binary'`.
- Failed attempts:
  - Forcing `return_dtype` to Object then casting to Date/Datetime/Binary.
  - Forcing `return_dtype` to per-column mapped dtypes.
  - Hybrid Object + downstream casts.
- Working fix (from another AI):
  - Remove `return_dtype` in `map_elements` and set `skip_nulls=False` so nulls still flow through the lambda.
  - Let Polars infer from the lambda outputs (datetime/date/bytes/None) instead of enforcing Object/Date/Binary builders.
  - Keep minimal normalization (NaN/None cleanup, binary→bytes) and schema-driven replication; no extra transformations.

### Lesson Learned

**"Trust the Source, Not the Sample":** For ETL workloads, always use explicit schemas from the source database rather than inferring from data samples. Inference works for analysis, but fails for production data pipelines with messy real-world data (mixed types, nulls, edge cases).

---

## Next Steps

1. **Testing the New Streaming Mode**

   - Test `replicate_reference_tables.py --full-table` on reference tables
   - Verify performance improvement vs. old Parquet mode
   - Confirm backward compatibility via shim files

2. **Update Cron Jobs / Scheduled Tasks**

   - Eventually migrate to new script names (shims provide time buffer)
   - Update documentation with migration timeline

3. **Continue Historical Data Load**
   - Use `replicate_monthly_parallel_streaming.py` to complete 2024 backfill
   - Monitor deadlock issues with worker count tuning

---

## Lessons Learned

1. **Naming Matters:** Clear, descriptive script names prevent confusion about which tool to use
2. **Refactor Over Rewrite:** Adding streaming to existing script preserved all helper functions and dependencies
3. **Backward Compatibility:** Shims allow gradual migration without breaking existing workflows
4. **Streaming is King:** Direct in-memory streaming eliminates disk I/O bottleneck for both small and large tables
5. **Idempotent by Design:** Delete-before-insert pattern ensures safe re-runs with no duplicates, making interrupted loads recoverable without manual cleanup
6. **Measure, Don't Assume:** "Obvious" optimizations (larger batches) can backfire. Always measure performance before and after changes. Table width matters for batch sizing.
7. **Go Native or Go Home:** Hybrid approaches (Polars→Pandas conversion) negate performance benefits. Full migration to a new library is worth the extra effort for true gains.
8. **Trust the Source, Not the Sample:** For ETL, explicit schemas from the source database beat type inference from data samples. Production data has mixed types, nulls, and edge cases that break inference-based approaches.
