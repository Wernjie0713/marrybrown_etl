## Command Reference

### Reference Tables (Full-Table)
```bash
# Stream reference tables directly into SQL (default)
python scripts/replicate_reference_tables.py --full-table

# Same as above but keep Parquet flow
python scripts/replicate_reference_tables.py --full-table --full-table-mode parquet

# Export only (no SQL load)
python scripts/replicate_reference_tables.py --full-table --skip-load
```

### Date-Filtered (Any Table)
```bash
# Load date window for specific tables
python scripts/replicate_reference_tables.py --start-date 2024-01-01 --end-date 2024-02-01 --table APP_4_SALES --table APP_4_SALESITEM
```

### Large Date-Based Tables (Monthly Parallel)
```bash
# Stream months in parallel
python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --max-workers 4

# Resume from checkpoint
python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --resume
```

### Orchestration (T-0 / T-1)
```bash
# Yesterday plus T-1 back-check
python scripts/run_replica_etl.py --date 2024-11-25

# Skip T-1
python scripts/run_replica_etl.py --date 2024-11-25 --skip-t1
```
