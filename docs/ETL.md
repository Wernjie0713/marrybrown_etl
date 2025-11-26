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

