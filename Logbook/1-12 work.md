# 1 December 2024 - Work Log

## Summary
Today's work focused on improving the ETL pipeline's date filtering logic and creating a new monthly parallel replication script to handle large date ranges that were causing connection timeouts.

---

## 1. Modified Date Filtering Logic in `export_and_load_replica.py`

### Issue
When using `--start-date` and `--end-date` flags, the script was attempting to process all tables, including reference tables that don't have date columns. This caused errors for tables like `APP_4_SALESDELIVERY`.

### Solution
Modified the `main()` function in `scripts/export_and_load_replica.py` (lines 1343-1361) to filter tables based on date range arguments:

- **When `--start-date/--end-date` are provided WITHOUT `--full-table`**: Only process tables listed in `DATE_FILTER_COLUMNS` (date-based tables)
- **When `--full-table` flag is used**: Only process reference tables (tables NOT in `DATE_FILTER_COLUMNS`)

### Code Changes
```python
# Filter tables based on date range (if provided without --full-table)
if start_date and not args.full_table:
    # When date ranges are provided, only process date-based tables
    date_based_tables = [t for t in tables if t in DATE_FILTER_COLUMNS]
    reference_tables = [t for t in tables if t not in DATE_FILTER_COLUMNS]
    
    if reference_tables:
        print(f"[INFO] Date range provided. Skipping reference tables (no date columns): {', '.join(reference_tables)}")
        print(f"[INFO] Use --full-table flag to process reference tables.")
    
    tables = date_based_tables
```

### Result
- Date-based tables are correctly identified and processed when date ranges are provided
- Reference tables are skipped with a clear message
- Users are informed to use `--full-table` flag for reference tables

---

## 2. Removed `APP_4_SALESDELIVERY` from Date Filtering

### Issue
The table `APP_4_SALESDELIVERY` was listed in `DATE_FILTER_COLUMNS` with column `DATETIME__SALES_DATE`, but this column doesn't actually exist in the table schema.

### Investigation
- Checked `docs/xilnex_full_schema.json` to verify column existence
- Confirmed that `APP_4_SALESDELIVERY` does not have a `DATETIME__SALES_DATE` column

### Solution
- Removed `APP_4_SALESDELIVERY` from `DATE_FILTER_COLUMNS` dictionary in `export_and_load_replica.py` (line 61-72)
- Created SQL script `scripts/empty_date_based_tables.sql` to empty date-based tables for testing

### Testing
Ran the table specifically with `--full-table` flag:
```bash
python scripts/export_and_load_replica.py --table APP_4_SALESDELIVERY --full-table
```

### Result
- Table exported successfully but contains **0 rows** (empty table)
- No errors encountered
- Table can be handled separately if needed in the future

---

## 3. Successful Replication of 5 Tables (2 Days Data)

### Tables Replicated
Successfully replicated the following date-based tables with 2 days of test data:
1. `APP_4_SALES`
2. `APP_4_SALESITEM`
3. `APP_4_PAYMENT`
4. `APP_4_VOIDSALESITEM`
5. `APP_4_SALESCREDITNOTE`
6. `APP_4_SALESCREDITNOTEITEM`
7. `APP_4_SALESDEBITNOTE`
8. `APP_4_SALESDEBITNOTEITEM`
9. `APP_4_EPAYMENTLOG`
10. `APP_4_VOUCHER`

### Command Used
```bash
python scripts/export_and_load_replica.py --start-date 2024-01-01 --end-date 2024-01-03
```

### Result
- All date-based tables processed successfully
- Data exported to Parquet and loaded into SQL Server warehouse
- No data quality issues encountered
- Confirmed the date filtering logic works correctly

---

## 4. Connection Timeout Issues with Full Year Replication

### Issue
When attempting to replicate a full year of data (2024-01-01 to 2024-12-31), all 10 date-based tables failed with connection timeout errors:

```
[08S01] Communication link failure (10060)
[08S01] Communication link failure (0)
```

### Root Cause
- **Large date range**: Full year queries return millions of rows
- **Long-running queries**: Source database takes too long to respond
- **Network timeout**: Connection drops before query completes
- **VPN/Network instability**: Connection may be unreliable for large datasets

### Solution: Created `replicate_monthly_parallel.py`

Created a new script that processes data month-by-month using parallel workers to avoid connection timeouts.

#### Approach

1. **Month-by-Month Processing**
   - Splits date range into individual months (e.g., 2024-01-01 to 2024-12-31 = 12 months)
   - Each month is processed independently

2. **Parallel Workers**
   - Uses `ThreadPoolExecutor` with configurable workers (default: 12)
   - Each worker processes one month simultaneously
   - Reduces total processing time significantly

3. **Incremental Parquet Writing**
   - Each month exports to a temporary Parquet file
   - After all months complete, merges into final Parquet file
   - Thread-safe merging using locks

4. **Resume Capability**
   - Saves checkpoint file (`{table}_monthly_checkpoint.json`) after each batch
   - On resume, analyzes existing Parquet to determine complete months
   - Only loads complete months to SQL (e.g., if stopped at 2024-03-22, only loads up to 2024-02-28)
   - Skips already-completed months

5. **Complete Month Detection**
   - Analyzes Parquet file to find maximum date
   - Only considers months complete if `max_date >= month_end`
   - If max_date falls within a month, that month is marked incomplete
   - Prevents loading partial month data

#### Key Features

- **Parallel Export**: Multiple months exported simultaneously
- **Thread-Safe Merging**: Uses locks to prevent concurrent write conflicts
- **Smart Resume**: Only processes incomplete months on resume
- **Data Integrity**: Only loads complete months to avoid partial data
- **Progress Tracking**: Clear output showing completed/failed months
- **Error Handling**: Individual month failures don't stop entire process

#### Usage

```bash
# Initial run - replicate full year
python scripts/replicate_monthly_parallel.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31

# Resume after interruption
python scripts/replicate_monthly_parallel.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --resume

# Export only (no SQL load)
python scripts/replicate_monthly_parallel.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --skip-load

# Custom number of workers
python scripts/replicate_monthly_parallel.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --max-workers 6
```

#### Implementation Details

**Functions Created:**
- `generate_month_ranges()`: Splits date range into month tuples
- `export_month()`: Exports one month to temp Parquet
- `merge_month_parquets()`: Thread-safe merging of month Parquets
- `get_complete_months_from_parquet()`: Analyzes Parquet to find complete months
- `load_checkpoint()` / `save_checkpoint()`: Resume functionality
- `replicate_monthly_parallel()`: Main orchestration function

**Checkpoint Format:**
```json
{
  "table": "APP_4_SALES",
  "completed_months": ["2024-01", "2024-02", "2024-03"],
  "failed_months": [],
  "final_parquet": "exports/app_4_sales/app_4_sales_monthly_20241201T120000Z.parquet",
  "last_updated": "2024-12-01T12:00:00Z"
}
```

**Resume Logic:**
1. Load checkpoint file if `--resume` flag is used
2. If existing Parquet exists, analyze it to find complete months
3. Skip completed months during export phase
4. Merge new month data with existing Parquet (append mode)
5. Only load data up to last complete month to SQL

### Current Status
- Script created and ready for testing
- Waiting for results from initial full-year replication run
- Expected to handle connection timeouts by processing smaller month-sized chunks

---

## 5. Merge Performance Optimization & Next Steps

### Improvement
- Optimized the merge phase in `scripts/replicate_monthly_parallel.py`.
- Replaced the old batch-by-batch pandas conversion with a PyArrow-native approach.
- The new version reads each month file directly as a PyArrow table, unifies schemas once, concatenates them in memory, and performs a single Parquet write.
- Added verbose `[MERGE]` progress messages to show which month is being processed and how many rows are merged.
- Clean-up still removes the monthly temp files after a successful merge.

### Remaining Issue
- Even with the faster merge, exporting large months over the unstable VPN remains slow (network-bound). Considering using SSIS to pull Parquet dumps directly from the Xilnex database for better throughput.

---

## 6. SSIS Staging Workflow (In Progress)

- Decided to offload bulk extraction to **SSIS**: Xilnex → SSIS Data Flow → warehouse staging tables → final replica.
- Started building the SSIS package (`XilnexParquetExport`):
  1. Created package variables (`User::StartDate`, `User::EndDate`, `User::TargetTable`, `User::MonthList`).
  2. Added Execute SQL Task to generate the 2024 month list (currently completed **Step 4.1: Execute SQL Task – build month list**).
  3. Next steps (still pending): configure ForEach Loop, Data Flow (OLE DB Source + staging destination), and post-load merge into replica tables.
- This approach will extract directly into SQL staging tables, eliminating Parquet for daily loads while still letting us reuse Python for nightly deltas.

---

## Files Modified/Created

### Modified
- `scripts/export_and_load_replica.py`
  - Added date filtering logic (lines 1343-1361)
  - Removed `APP_4_SALESDELIVERY` from `DATE_FILTER_COLUMNS`

### Created
- `scripts/replicate_monthly_parallel.py` - New monthly parallel replication script
- `scripts/empty_date_based_tables.sql` - SQL script to empty date-based tables for testing
- `1-12 work.md` - This work log

---

## Next Steps

1. **Monitor Monthly Parallel Script**
   - Wait for initial full-year replication to complete
   - Verify all months are processed correctly
   - Confirm data integrity (complete months only loaded)

2. **Performance Optimization** (if needed)
   - Adjust `--max-workers` based on system resources
   - Fine-tune `--chunk-size` for optimal memory usage
   - Consider batch processing multiple tables

3. **Documentation**
   - Update main README with monthly parallel script usage
   - Document resume scenarios and best practices

4. **Testing**
   - Test resume functionality with simulated interruptions
   - Verify complete month detection logic
   - Test with various date ranges (partial years, cross-year boundaries)

---

## Lessons Learned

1. **Connection Timeouts**: Large date ranges cause connection timeouts - need to chunk by time periods
2. **Schema Validation**: Always verify column existence in schema before adding to filter dictionaries
3. **Resume Capability**: Critical for long-running ETL jobs - checkpoint/resume prevents data loss
4. **Data Integrity**: Only loading complete months prevents partial data issues in analytics
5. **Parallel Processing**: Month-by-month parallel processing significantly reduces total time while avoiding connection issues
