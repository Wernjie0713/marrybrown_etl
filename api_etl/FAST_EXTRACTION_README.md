# Fast Sample Data Extraction

**Purpose**: Quickly extract sample data from Xilnex API with optimized parallel database writes.

## Key Features

✅ **Sequential API Calls** - Respects API's timestamp pagination requirement  
✅ **Parallel DB Writes** - Writes to 3 tables (sales, items, payments) concurrently  
✅ **Simple INSERT** - Faster than MERGE for sample data  
✅ **Large Batch Accumulation** - Accumulates 25K records before writing (fewer DB round trips)  
✅ **Resume Capability** - Uses `api_sync_metadata` to store `lastTimestamp` for crash recovery  
✅ **Correct API Usage** - Uses only `starttimestamp` parameter (no date filtering in API)  

## Performance

- **Expected Speedup**: 2-4x faster than chunked extraction
- **API Calls**: Sequential (required by API) - ~12 seconds per call
- **DB Writes**: Parallel (3 tables) - ~10-15 seconds per batch
- **Total Time**: ~75-80 minutes for 1 year of data (365 API calls)

## Usage

### Basic Usage

```bash
# Extract 1 year of data (default: 2024-01-01 to 2024-12-31)
python api_etl/extract_fast_sample.py

# Extract specific date range
python api_etl/extract_fast_sample.py 2024-01-01 2024-12-31

# Extract with API call limit (for testing)
python api_etl/extract_fast_sample.py 2024-01-01 2024-12-31 100
```

### Parameters

1. **start_date** (required): Start date in YYYY-MM-DD format
2. **end_date** (required): End date in YYYY-MM-DD format  
3. **max_calls** (optional): Maximum API calls (None for unlimited)

### Example: Extract 1 Year of Sample Data

```bash
python api_etl/extract_fast_sample.py 2024-01-01 2024-12-31
```

This will:
- Make sequential API calls (respecting timestamp pagination)
- Accumulate 25K records per batch
- Write to database in parallel (3 tables simultaneously)
- Show progress after each batch
- Complete in ~75-80 minutes

## Configuration

Edit `extract_fast_sample.py` to adjust:

```python
BATCH_ACCUMULATION_SIZE = 25000  # Records per batch (default: 25K)
ENABLE_EARLY_EXIT = True         # Stop when date range exceeded
BUFFER_DAYS = 7                  # Continue 7 days past end_date
```

## How It Works

### 1. Sequential API Calls (Required)
```
Call 1: GET /apps/v2/sync/sales?limit=1000
  └─> Response includes lastTimestamp (hex value)
Call 2: GET /apps/v2/sync/sales?limit=1000&starttimestamp=<lastTimestamp>
  └─> Get next lastTimestamp
... (continue sequentially until API returns empty)
```

**Important**: The API does NOT support date range filtering. Date parameters are NOT sent to the API. Date filtering happens client-side only (for early exit logic).

### 2. Resume Capability
- On startup, checks `api_sync_metadata` for existing `lastTimestamp`
- If found, resumes from that timestamp (no duplicate data)
- After each batch write, saves `lastTimestamp` to metadata
- Enables crash recovery - can resume after interruption

### 3. Batch Accumulation
- Accumulates sales records in memory
- When batch reaches 25K records, triggers write
- Saves checkpoint to `api_sync_metadata` after each write

### 4. Parallel DB Writes (Optimization)
```
ThreadPoolExecutor (3 workers):
  ├─> Worker 1: INSERT into staging_sales
  ├─> Worker 2: INSERT into staging_sales_items
  └─> Worker 3: INSERT into staging_payments
```

### 5. Data Transformation
- Sales → DataFrame (with LocationKey resolution)
- Items → DataFrame (with SaleID linkage)
- Payments → DataFrame (with SaleID linkage)
- Complex fields serialized to JSON

## Output

The script writes to staging tables:
- `dbo.staging_sales`
- `dbo.staging_sales_items`
- `dbo.staging_payments`

## Resume Capability

The script now supports resume capability via `api_sync_metadata`:

- **Automatic Resume**: On startup, checks for previous run and resumes from `lastTimestamp`
- **Checkpoint After Each Batch**: Saves progress after every 25K records
- **Crash Recovery**: If interrupted, can resume from last checkpoint
- **Status Tracking**: Tracks status (IN_PROGRESS, COMPLETED, INTERRUPTED)

To disable resume (start fresh):
```python
extract_fast_sample(start_date, end_date, resume=False)
```

## Limitations

⚠️ **Not Production-Ready**:
- No idempotency (uses INSERT, not MERGE - may create duplicates on rerun)
- No quality validation
- Basic error handling

✅ **Use For**:
- Quick sample data extraction
- Schema testing
- Fast iteration during development
- Resume capability makes it safer for long-running extractions

## Comparison with Chunked Extraction

| Feature | Fast Extraction | Chunked Extraction |
|---------|----------------|-------------------|
| API Calls | Sequential | Sequential |
| DB Writes | Parallel (3 tables) | Sequential |
| Batch Size | 25K records | 50 API calls (~50K) |
| Idempotency | No (INSERT) | Yes (MERGE) |
| Resume | Yes (via metadata) | Yes (via metadata) |
| Quality Check | No | Yes |
| Speed | 2-4x faster | Baseline |
| Use Case | Sample data | Production |

## Troubleshooting

### Connection Timeout
- Check `.env.cloud` file has correct database credentials
- Verify network connectivity to database server

### API Errors
- Check `config_api.py` has correct API credentials
- Verify API host is accessible

### Memory Issues
- Reduce `BATCH_ACCUMULATION_SIZE` if running out of memory
- Default 25K should work for most systems

## Next Steps

After extraction:
1. Verify data in staging tables
2. Run transformation: `python api_etl/transform_api_to_facts.py`
3. Check fact tables for transformed data

## Author

YONG WERN JIE  
Date: December 2025

