# Work Log - 4th December

## Summary

Today's focus was on orchestrating the monthly streaming replication for all sales-related tables. We created a master script to run these replications sequentially, ensuring a safe and controlled load on the source database while maintaining parallel performance within each table.

We also successfully deployed the ETL environment to the Windows VM, resolving several database connectivity and configuration issues.

---

## 1. Orchestration Script Implementation

### New Script: `scripts/replicate_all_sales_data.py`

We created a new orchestration script to automate the replication of all sales tables that support date filtering.

### Key Features

- **Sequential Execution:** Runs tables one after another to prevent "multiplicative load" (e.g., 10 tables x 2 workers = 20 connections) which could overwhelm the source OLTP system.
- **Parallelism within Tables:** Passes the `--max-workers` argument to the underlying `replicate_monthly_parallel` function, allowing each table to still benefit from parallel month processing.
- **Targeted Scope:** Automatically iterates through the specific sales tables configured in `DATE_FILTER_COLUMNS`.
- **Exclusions:** Explicitly excludes `APP_4_ITEM` and `APP_4_STOCK` as they are better handled as reference tables (full refresh) due to their specific update patterns.

### Usage

```bash
python scripts/replicate_all_sales_data.py --start-date 2025-10-01 --end-date 2025-11-30 --max-workers 2
```

### Tables Included

The script automatically processes the following tables:

1.  `APP_4_SALES`
2.  `APP_4_SALESITEM`
3.  `APP_4_PAYMENT`
4.  `APP_4_VOIDSALESITEM`
5.  `APP_4_SALESCREDITNOTE`
6.  `APP_4_SALESCREDITNOTEITEM`
7.  `APP_4_SALESDEBITNOTE`
8.  `APP_4_SALESDEBITNOTEITEM`
9.  `APP_4_EPAYMENTLOG`
10. `APP_4_VOUCHER`

---

## 2. Technical Decisions

### Why Sequential Tables?

We considered running all tables in parallel but decided against it because:

- **Source Risk:** Hitting the Xilnex OLTP database with 20+ simultaneous heavy read streams (10 tables \* 2 workers) poses a high risk of slowing down POS operations.
- **Target Risk:** Previous testing showed deadlocks with just 4 workers on a single table. 20 concurrent writers would likely saturate disk I/O and cause timeouts.
- **Stability:** Sequential execution with 2 workers per table provides a stable, consistent load that is safe for both source and target systems.

### Handling Item and Stock Tables

`APP_4_ITEM` and `APP_4_STOCK` were excluded from this streaming pipeline. They will continue to be replicated using `replicate_reference_tables.py` (likely in `--full-table` mode) as they are reference data that doesn't fit the monthly date-partitioned pattern as cleanly as transaction tables.

---

## 3. VM Deployment & Troubleshooting

We successfully deployed the code to the Windows VM and established connectivity.

### Setup Steps

1.  Installed Git, Python 3.13 (added to PATH), and ODBC Driver 17.
2.  Cloned the repository and set up a virtual environment (`venv`).
3.  Installed dependencies (added `polars` to `requirements.txt`).
4.  Configured `.env` with `TARGET_SERVER=127.0.0.1`.

### Issues Resolved

#### 1. Database Login Failure (`Login failed for user 'etl_user'`)

- **Cause:** Mixed Mode Authentication was enabled but the SQL Server service hadn't been restarted, and the user password might have been mismatched.
- **Fix:**
  - Restarted SQL Server Service.
  - Ran `migrations/setup_etl_user.sql` and `migrations/fix_login.sql` to ensure the user existed and was enabled.

#### 2. Connection Error (`Named Pipes Provider: Could not open a connection... [53]`)

- **Cause:** The driver couldn't resolve `localhost` correctly or was defaulting to Named Pipes.
- **Fix:**
  - Verified TCP/IP was enabled in SQL Server Configuration Manager.
  - Changed `.env` to use `TARGET_SERVER=127.0.0.1` (forcing IPv4).

#### 3. Test Script Failure (`invalid literal for int()`)

- **Cause:** The test script `tests/test_connections.py` used SQLAlchemy, which failed to parse the `tcp:` prefix we initially tried.
- **Fix:** Refactored `tests/test_connections.py` to use `pyodbc` directly, matching the production script's behavior. This confirmed that `127.0.0.1` works correctly for both.

#### 4. Wrong Environment File

- **Cause:** The test script was prioritizing `.env.local` over `.env`.
- **Fix:** Deleted `.env.local` on the VM and updated `.gitignore` to exclude environment files.
