## ETL Process – 1:1 Replica Workflow

### Overview

- **Source**: Xilnex production database (read-only over VPN).
- **Target**: `MarryBrown_DW` (SQL Server 2022) hosted on TIMEdotcom cloud.
- **Pattern**: Direct copy of source tables; no transformations beyond data fix checks.

### Daily Schedule (Automated)

| Time  | Action                                                                         |
| ----- | ------------------------------------------------------------------------------ |
| 02:00 | Extract **T-0** (yesterday) data for all replicated tables.                    |
| 02:15 | Re-query **T-1** (day before) to detect late fixes/voids + UPSERT accordingly. |
| 02:30 | Log run metadata (records extracted, fixes applied) into `etl_progress`.       |

### Manual Trigger Examples (planned CLI)

```bash
# single day refresh
python run_replica_etl.py --date 2025-11-26

# date range backfill
python run_replica_etl.py --start 2024-01-01 --end 2024-12-31

# T-1 fix check only
python run_replica_etl.py --check-fixes --date 2025-11-25
```

### Tables Replicated (Phase 1 - 19 tables)

**Sales & Transactions:**

- `APP_4_SALES` (184 columns)
- `APP_4_SALESITEM` (197 columns)
- `APP_4_PAYMENT` (70 columns)
- `APP_4_VOIDSALESITEM` (36 columns)
- `APP_4_SALESCREDITNOTE` (100 columns)
- `APP_4_SALESCREDITNOTEITEM` (132 columns)
- `APP_4_SALESDEBITNOTE` (100 columns)
- `APP_4_SALESDEBITNOTEITEM` (132 columns)
- `APP_4_SALESDELIVERY` (23 columns)
- `APP_4_EXTENDEDSALESITEM` (6 columns)
- `APP_4_EPAYMENTLOG` (20 columns)
- `APP_4_VOUCHER` (67 columns)
- `APP_4_CASHIER_DRAWER` (29 columns)

**Products & Inventory:**

- `APP_4_ITEM` (190 columns)
- `APP_4_STOCK` (47 columns)

**Customers & Loyalty:**

- `APP_4_CUSTOMER` (160 columns)
- `APP_4_POINTRECORD` (36 columns)

**Outlets:**

- `LOCATION_DETAIL` (14 columns)

**Promotions:**

- `APP_4_VOUCHER_MASTER` (64 columns)

### Data Fix Logic

1. Pull T-1 rows from source (filtered by `DATETIME__SALES_DATE`).
2. Compare hashes / row counts against warehouse.
3. Apply `MERGE`/`DELETE` so warehouse reflects any corrections.

### Post-Load Optimization (allowed even with raw schema)

- Add covering indexes on high-volume query columns (date, location, sale number).
- Create views that convert varchar dates to DATETIME for API consumption.
- Track load stats + data quality metrics in `dimension_refresh_audit`.

### Implementation Notes

**Schema Generation:**

- Migration files are generated from actual Xilnex schema using `scripts/generate_migration_from_schema.py`
- This ensures we replicate **exactly** what exists in Xilnex (all columns, correct types)
- Run migrations: `000_drop_all_tables.sql` (if needed), then `100_create_replica_tables.sql`, then `110_create_replica_metadata_tables.sql`

**Export & Load Script:**

- `scripts/replicate_reference_tables.py` reads **actual columns from `docs/xilnex_full_schema.json`** (not `replica_schema.json`)
- `replica_schema.json` is for API development reference only; replication uses the full schema
- Default `--full-table` path streams reference tables directly into SQL (no Parquet); keep the old Parquet flow by setting `--full-table-mode parquet`
- **New:** `--skip-existing` flag skips tables that are already loaded (checks target database)
- Backward-compat wrapper: `scripts/export_and_load_replica.py` imports/forwards to `replicate_reference_tables.py`

**Examples:**

```bash
# Full table load (reference tables, no date filter)
python scripts/replicate_reference_tables.py --full-table

# Date-filtered load (2024-2025 data)
python scripts/replicate_reference_tables.py --start-date 2024-01-01 --end-date 2026-01-01

# Specific tables with skip logic
python scripts/replicate_reference_tables.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-02 \
  --table APP_4_SALES \
  --table APP_4_SALESITEM \
  --skip-existing

# Export only (no load)
python scripts/replicate_reference_tables.py --full-table --skip-load

# Force legacy Parquet mode for full-table runs
python scripts/replicate_reference_tables.py --full-table --full-table-mode parquet

# Monthly parallel streamer for large date tables
python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --max-workers 2
```

**Idempotent Loads (Safe Re-runs):**

Both replication scripts implement a **delete-before-insert** pattern for safe, idempotent loads:

1. **Before Loading:** Each date range (or full table) is **deleted** from the target before inserting new data
2. **Per-Month Scoping:** `replicate_monthly_parallel_streaming.py` deletes each month's range independently
3. **Retry-Safe:** If a month fails and retries, the delete runs again before the retry insert
4. **No Duplicates:** Running the same date range multiple times will NOT create duplicates

**What This Means:**

- ✅ You can safely re-run any date range to refresh data
- ✅ Interrupted runs can be restarted without cleanup
- ✅ No risk of duplicate rows (as long as the date column in `DATE_FILTER_COLUMNS` is correct)
- ✅ Always gets the latest data from source

**How It Works:**

```python
# For each month (or date range):
DELETE FROM dbo.com_5013_APP_4_SALES
WHERE DATETIME__SALES_DATE >= '2024-01-01'
  AND DATETIME__SALES_DATE < '2024-02-01'

# Then insert fresh data for that range
INSERT INTO dbo.com_5013_APP_4_SALES WITH (TABLOCK) ...
```

**Index Management:**

- Nonclustered indexes are **disabled after the delete** (so delete runs with indexes present)
- Indexes are **rebuilt after successful load** (or in cleanup on error)
- This speeds up bulk inserts significantly

**Orchestration:**

- `scripts/run_replica_etl.py` orchestrates the nightly T-0/T-1 workflow
- Records outcomes in `replica_run_history` table

```bash
# Run yesterday's load plus T-1 fix check
python scripts/run_replica_etl.py --date 2024-11-25

# Skip T-1 check
python scripts/run_replica_etl.py --date 2024-11-25 --skip-t1
```

**File Locations:**

- Streaming runs (full-table default) write a JSON manifest next to the table output directory; Parquet manifests still write alongside the file when that mode is enabled
- Parquet exports: `exports/<table>/` (only when Parquet mode is used; configurable via `--output-dir`)
- Each Parquet run writes a JSON manifest (row counts, file path, timestamps) next to the Parquet file
- Connection settings: `.env.local` for local testing, `.env` for production
