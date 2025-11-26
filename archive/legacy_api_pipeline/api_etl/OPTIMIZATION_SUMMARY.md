# ETL Optimization Summary

**Author**: YONG WERN JIE  
**Date**: December 2025  
**Status**: ✅ Implemented and Tested

---

## Overview

This document summarizes all optimizations implemented in the MarryBrown ETL pipeline to improve extraction and transformation performance. These optimizations have been tested and are currently in use.

---

## 1. Fast Sample Data Extraction (`extract_fast_sample.py`)

### Implemented Optimizations

#### 1.1 Parallel Database Writes
- **Before**: Sequential writes to 3 staging tables (sales, items, payments)
- **After**: Parallel writes using `ThreadPoolExecutor` with 3 workers
- **Impact**: 2-3x faster database writes
- **Implementation**: Each table write runs in a separate thread simultaneously

#### 1.2 Large Batch Accumulation
- **Before**: Small batches or immediate writes after each API call
- **After**: Accumulates 25,000 records before writing to database
- **Impact**: Reduces database round trips by ~25x
- **Configuration**: `BATCH_ACCUMULATION_SIZE = 25000`

#### 1.3 Resume Capability
- **Feature**: Automatic resume from last checkpoint using `api_sync_metadata`
- **Impact**: Enables crash recovery without data loss or duplication
- **Implementation**: Stores `lastTimestamp` after each batch write
- **Status Tracking**: Tracks IN_PROGRESS, COMPLETED, INTERRUPTED states

#### 1.4 Smart Early Exit (Removed)
- **Status Update**: Early-exit logic has been removed; extraction now always scans the full requested window so historical backfills are never skipped.
- **Impact**: Slightly more API calls during wide replays, but completeness is guaranteed.
- **Note**: `ENABLE_EARLY_EXIT` / `BUFFER_DAYS` settings are no longer referenced in code.

#### 1.5 Configurable API Call Limits
- **Feature**: Uses `MAX_API_CALLS` from config (defaults to 2 for testing)
- **Impact**: Allows quick testing with limited calls
- **Configuration**: Set in `config_api.py` (currently 2 for testing)

#### 1.6 Network Error Handling & Retry Logic
- **Feature**: Robust handling of network errors with exponential backoff retry
- **Errors Handled**: `ChunkedEncodingError`, `ProtocolError`, `IncompleteRead`
- **Implementation**: 
  - Force-reads `response.content` immediately to catch incomplete reads before JSON parsing
  - Exponential backoff retry with jitter (2s, 4s, 8s, 16s, 32s)
  - Configurable retry attempts via `API_MAX_RETRIES` (default: 5)
- **Impact**: Pipeline survives flaky VPN conditions, connection drops, and incomplete HTTP transfers
- **Files**: Applied to both `extract_fast_sample.py` and `extract_from_api_chunked.py`

---

## 2. Transform Phase Optimizations (`transform_api_to_facts.py`)

### Implemented Optimizations

#### 2.1 Chunked MERGE Processing
- **Before**: Single large MERGE operation on entire staging dataset
- **After**: Processes data in chunks of 50,000 sales records
- **Impact**: 
  - Prevents memory issues with large datasets
  - Faster MERGE operations (smaller working sets)
  - Better transaction management
- **Configuration**: `chunk_size=50000` (default)

#### 2.2 Connection Pooling
- **Before**: NullPool (no connection reuse)
- **After**: SQLAlchemy connection pooling with pre-ping
- **Impact**: 
  - Reuses database connections
  - Automatic reconnection on stale connections
  - Reduced connection overhead
- **Implementation**: `pool_pre_ping=True` in engine creation

#### 2.3 Vectorized Pandas Operations
- **Before**: Row-by-row processing with `.apply()` methods
- **After**: Column-level vectorized operations
- **Impact**: 10-100x faster data transformations
- **Areas Optimized**:
  - LocationKey lookup (batch query instead of per-row)
  - Decimal conversion (column-level operations)
  - Data normalization (vectorized pandas operations)

#### 2.4 Batch LocationKey Lookup
- **Before**: Individual database query for each outlet location
- **After**: Single batch query to fetch all LocationKeys at once
- **Impact**: Reduces database queries from N to 1 (where N = number of unique outlets)
- **Implementation**: Uses `pd.merge()` with pre-fetched location mapping

#### 2.5 Vectorized Decimal Conversion
- **Before**: Row-by-row decimal conversion using `.apply()`
- **After**: Column-level decimal conversion using pandas operations
- **Impact**: Significantly faster type conversions
- **Implementation**: Direct column assignment with decimal conversion

#### 2.6 Staging Table Indexes
- **Feature**: Optimized indexes on staging tables for faster MERGE operations
- **Impact**: Faster JOIN operations during transform
- **Implementation**: SQL script creates indexes on key columns (SaleID, BusinessDateTime, etc.)

#### 2.7 Proper Cleanup Execution
- **Feature**: Fixed indentation error ensuring `cleanup_staging()` runs correctly after transformation
- **Impact**: Ensures staging data cleanup always executes, preventing stale data accumulation
- **Implementation**: Cleanup code now runs at function level after completion messages

#### 2.8 Chronological Ordering for TransactionKey
- **Before**: Chunked processing ordered by `SaleID` (arbitrary order)
- **After**: Chunked processing ordered by `BusinessDateTime, SaleID` (chronological order)
- **Impact**: 
  - TransactionKey values follow chronological order, making data more logical and easier to debug
  - Better alignment with business logic (date-based ordering)
  - Minimal performance impact due to existing `IX_staging_sales_BusinessDateTime` index
- **Implementation**: Changed `ROW_NUMBER() OVER (ORDER BY ss.SaleID)` to `ROW_NUMBER() OVER (ORDER BY ss.BusinessDateTime, ss.SaleID)`
- **Performance**: Negligible impact (~5-10% overhead) as the existing index on `BusinessDateTime` is utilized for sorting
- **Known Limitation**: `TransactionKey` still represents load sequence; if older facts arrive late (historical replay, API out-of-order), the key can appear out of chronological order. Downstream consumers should sort by `BusinessDateTime`/`DateKey` when strict chronology is required.

#### 2.9 Fact Granularity Alignment (Nov 2025 Hotfix)
- **Issue**: MERGE occasionally failed with "multiple source rows matched" when split-tender allocations produced duplicate fact grain rows.
- **Fix**: Added an `AggregatedData` CTE that groups to `SaleNumber + DateKey + ProductKey + PaymentTypeKey` before the MERGE.
- **Impact**: MERGE is now deterministic/idempotent and matches the fact table's composite key even with multi-payment sales.

---

## 3. Configuration Optimizations

### 3.1 Environment-Based Configuration
- **Feature**: Separate configurations for local and cloud environments
- **Files**: `.env.local` (local development), `.env.cloud` (production)
- **Impact**: Easy switching between environments without code changes

### 3.2 Timeout Configuration
- **Feature**: Extended timeouts for slow VPN connections
- **Settings**: 
  - Connection timeout: 60 seconds
  - Login timeout: 60 seconds
- **Impact**: Prevents timeout errors on slow networks

### 3.3 Staging Retention Policy
- **Feature**: Automatic cleanup of old staging data
- **Configuration**: `STAGING_RETENTION_DAYS = 14` (default)
- **Impact**: Keeps staging tables lean for faster MERGE operations

---

## Performance Improvements Summary

### Extraction Phase
- **Parallel DB Writes**: 2-3x faster database writes
- **Batch Accumulation**: ~25x fewer database round trips
- **Total Extraction Speed**: 2-4x faster than original chunked extraction

### Transform Phase
- **Chunked Processing**: Handles large datasets without memory issues
- **Vectorization**: 10-100x faster data transformations
- **Connection Pooling**: Reduced connection overhead
- **Batch Lookups**: Eliminated N+1 query problem

### Overall Pipeline
- **End-to-End Speed**: 3-5x faster than original implementation
- **Memory Usage**: More efficient with chunked processing
- **Reliability**: Improved with resume capability, connection pooling, and network error handling

---

## Testing Configuration

### Current Testing Settings
- **MAX_API_CALLS**: 2 (for quick testing)
- **BATCH_ACCUMULATION_SIZE**: 25,000 records
- **Chunk Size**: 50,000 sales records per transform chunk

### Production Settings
- **MAX_API_CALLS**: None (unlimited for full extraction)
- **BATCH_ACCUMULATION_SIZE**: 25,000 records (optimal balance)
- **Chunk Size**: 50,000 records (optimal for MERGE performance)

---

## Files Modified

1. **`extract_fast_sample.py`**
   - Added parallel database writes
   - Implemented batch accumulation
   - Added resume capability
   - Integrated MAX_API_CALLS from config
   - Added network error handling with exponential backoff retry

2. **`transform_api_to_facts.py`**
   - Implemented chunked MERGE processing
   - Added connection pooling
   - Vectorized pandas operations
   - Batch LocationKey lookup
   - Vectorized decimal conversion
   - Fixed cleanup execution to ensure proper staging data cleanup
   - Chronological ordering for TransactionKey (BusinessDateTime-based chunking)

3. **`config_api.py`**
   - Added MAX_API_CALLS configuration (default: 2 for testing)
   - Documented configuration options

4. **`extract_from_api_chunked.py`**
   - Added network error handling with exponential backoff retry (for consistency)

---

## Future Optimization Opportunities

See `ADVANCED_OPTIMIZATIONS.md` for additional optimization opportunities that are documented but not yet implemented:

1. Parallel chunk processing in transform phase
2. Bulk insert methods (BULK INSERT / bcp)
3. Temporary tables for transform
4. Disable indexes during bulk load
5. Partitioned tables for fact table
6. Columnstore indexes
7. In-Memory OLTP tables
8. Async/Await for I/O operations

---

## Best Practices Implemented

1. **Idempotency**: MERGE operations ensure no duplicates
2. **Resume Capability**: Can recover from interruptions
3. **Connection Management**: Proper pooling and timeout handling
4. **Memory Efficiency**: Chunked processing prevents OOM errors
5. **Error Handling**: Graceful handling of network and database errors with exponential backoff retry
6. **Network Resilience**: Handles ChunkedEncodingError, ProtocolError, and IncompleteRead with automatic retries
7. **Monitoring**: Progress tracking and status updates
8. **Configuration**: Environment-based configuration management
9. **Resource Cleanup**: Proper cleanup execution ensures staging data doesn't accumulate

---

## Notes

- All optimizations have been tested with production-like data volumes
- Performance improvements vary based on dataset size and network conditions
- Some optimizations have diminishing returns - measure before implementing additional ones
- Always validate data quality after optimization changes

---

## References

- `FAST_EXTRACTION_README.md` - Detailed documentation for fast extraction
- `ADVANCED_OPTIMIZATIONS.md` - Future optimization opportunities
- `config_api.py` - Configuration settings
- `extract_fast_sample.py` - Optimized extraction implementation
- `transform_api_to_facts.py` - Optimized transform implementation

