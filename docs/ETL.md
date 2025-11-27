## ETL Process – 1:1 Replica Workflow

### Overview
- **Source**: Xilnex production database (read-only over VPN).
- **Target**: `MarryBrown_DW` (SQL Server 2022) hosted on TIMEdotcom cloud.
- **Pattern**: Direct copy of source tables; no transformations beyond data fix checks.

### Daily Schedule (Automated)
| Time | Action |
|------|--------|
| 02:00 | Extract **T-0** (yesterday) data for all replicated tables. |
| 02:15 | Re-query **T-1** (day before) to detect late fixes/voids + UPSERT accordingly. |
| 02:30 | Log run metadata (records extracted, fixes applied) into `etl_progress`. |

### Manual Trigger Examples (planned CLI)
```bash
# single day refresh
python run_replica_etl.py --date 2025-11-26

# date range backfill
python run_replica_etl.py --start 2024-01-01 --end 2024-12-31

# T-1 fix check only
python run_replica_etl.py --check-fixes --date 2025-11-25
```

### Tables Replicated (Phase 1)
- `APP_4_SALES`
- `APP_4_SALESITEM`
- `APP_4_PAYMENT`
- `APP_4_ITEM`
- `APP_4_CUSTOMER`
- `LOCATION_DETAIL`
- `APP_4_VOUCHER_MASTER`
- `APP_4_STAFF`
- `APP_4_TERMINAL` (if exposed)

### Data Fix Logic
1. Pull T-1 rows from source (filtered by `DATETIME__SALES_DATE`).
2. Compare hashes / row counts against warehouse.
3. Apply `MERGE`/`DELETE` so warehouse reflects any corrections.

### Post-Load Optimization (allowed even with raw schema)
- Add covering indexes on high-volume query columns (date, location, sale number).
- Create views that convert varchar dates to DATETIME for API consumption.
- Track load stats + data quality metrics in `dimension_refresh_audit`.

### Implementation Notes
- Run the schema migrations under `migrations/schema_tables/100_create_replica_tables.sql` and `110_create_replica_metadata_tables.sql` against `MarryBrown_DW` (use `.env.local` credentials with `scripts/run_migration.py`).
- `scripts/export_and_load_replica.py` performs the heavy lifting: it reads table/column definitions from `docs/replica_schema.json`, exports each table to Snappy Parquet, validates columns, writes a manifest, and inserts into the replica tables. Example:
  ```bash
  python scripts/export_and_load_replica.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-02 \
    --table APP_4_SALES \
    --table APP_4_SALESITEM
  ```
- `scripts/run_replica_etl.py` orchestrates the nightly job. It runs the T-0 export/load, optionally replays T-1, and records outcomes in `replica_run_history`.
  ```bash
  # run yesterday’s load plus T-1 fix check
  python scripts/run_replica_etl.py --date 2024-11-25
  ```
- All Parquet exports live under `exports/<table>/` by default. Each run also writes a JSON manifest (row counts, file path, timestamps) next to the Parquet file for auditing.

