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
