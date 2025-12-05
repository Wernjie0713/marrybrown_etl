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

---

## 4. VM Replication Success ✅

After resolving the connectivity issues, we successfully ran the replication script on the VM targeting the cloud warehouse.

### Command Executed

```powershell
python scripts/replicate_all_sales_data.py --start-date 2025-10-01 --end-date 2025-10-02 --max-workers 2
```

### Verification Results

We created `tests/verify_replication.py` to check row counts on the cloud warehouse (`10.0.1.194:1433`):

| Table                     | Row Count   | Status |
| ------------------------- | ----------- | ------ |
| APP_4_SALES               | 44,579      | ✓      |
| APP_4_SALESITEM           | 227,418     | ✓      |
| APP_4_PAYMENT             | 44,125      | ✓      |
| APP_4_VOIDSALESITEM       | 2,912       | ✓      |
| APP_4_SALESCREDITNOTE     | 0           | ○      |
| APP_4_SALESCREDITNOTEITEM | 0           | ○      |
| APP_4_SALESDEBITNOTE      | 0           | ○      |
| APP_4_SALESDEBITNOTEITEM  | 0           | ○      |
| APP_4_EPAYMENTLOG         | 0           | ○      |
| APP_4_VOUCHER             | 73,670      | ✓      |
| **TOTAL**                 | **392,704** |        |

### Notes

- Tables with 0 rows have no data in the **source** Xilnex database (not a replication issue).
- The replication pipeline is working correctly for October 2025 data.

---

## 5. Xilnex Source Database Issue & Local Verification

### Issue Reported by Xilnex Support

After the successful replication run, Xilnex support contacted us to report that we were querying the **primary (production) database** instead of their **replica database**. Our heavy read queries were affecting the live POS system performance.

**Action Required:** Switch to the Xilnex replica database server for all future replication runs.

**Current Status:** Waiting for Xilnex support to provide the connection string for the replica server.

### Created Local Reference Table Verifier

While waiting for Xilnex to respond, we continued progress by creating a verification script for the **local warehouse** (development machine).

**New Script:** `tests/verify_local_reference_tables.py`

**Features:**

- Loads reference table names dynamically from `replicate_reference_tables.py` (all tables NOT in `DATE_FILTER_COLUMNS`).
- Connects to the local warehouse (`localhost`, `MarryBrown_DW`, `etl_user`).
- Prints per-table row counts and a total in a clear table format (same style as `verify_replication.py`).
- Uses the standard `dbo.com_5013_` prefix for table names.

**Purpose:** Verify that reference tables (e.g., `APP_4_ITEM`, `APP_4_CUSTOMER`, `LOCATION_DETAIL`, `APP_4_STOCK`) have been successfully replicated to the local development warehouse.

### Current Data Distribution

| Warehouse       | Location     | Tables                          | Status       |
| --------------- | ------------ | ------------------------------- | ------------ |
| **Local (Dev)** | `localhost`  | Reference tables (no date cols) | ✓ Replicated |
| **Cloud (VM)**  | `10.0.1.194` | Sales tables (with date cols)   | ✓ Replicated |

### Local Reference Table Verification Results

Ran `python tests/verify_local_reference_tables.py`:

| Table                   |     Row Count | Status |
| ----------------------- | ------------: | ------ |
| APP_4_CASHIER_DRAWER    |         3,102 | ✓      |
| APP_4_CUSTOMER          |       905,321 | ✓      |
| APP_4_EXTENDEDSALESITEM |             0 | ○      |
| APP_4_ITEM              |        10,304 | ✓      |
| APP_4_POINTRECORD       |     3,734,108 | ✓      |
| APP_4_SALESDELIVERY     |             0 | ○      |
| APP_4_STOCK             |       175,297 | ✓      |
| APP_4_VOUCHER_MASTER    |         1,806 | ✓      |
| LOCATION_DETAIL         |           315 | ✓      |
| **TOTAL**               | **4,830,253** |        |

**Notes:**

- Tables with 0 rows (`APP_4_EXTENDEDSALESITEM`, `APP_4_SALESDELIVERY`) are empty in the source Xilnex database.
- All reference tables have been successfully replicated to the local development warehouse.

---

## 6. Sample Data Export for API Development

### Purpose

To speed up API development, we created a script that exports **sample rows** from all replicated tables as **JSON files**. These JSON files serve as a quick reference for understanding column names, data types, and real data examples—without needing to query the database every time.

### New Script: `scripts/export_sample_data.py`

**Features:**

- Connects to **cloud warehouse** for sales tables and **local warehouse** for reference tables.
- Fetches `TOP N` rows per table (default: 10, configurable via `--sample-size`).
- Exports nicely indented JSON files under `exports/sample_data/sales/` and `exports/sample_data/reference/`.
- Handles data type conversion (datetime → ISO string, decimal → string, binary → hex) for JSON compatibility.

### Usage

```bash
python scripts/export_sample_data.py --sample-size 10 --output-dir exports/sample_data
```

### Export Results

**Sales Tables (Cloud):**

| Table                     | Rows Exported |
| ------------------------- | ------------: |
| APP_4_SALES               |            10 |
| APP_4_SALESITEM           |            10 |
| APP_4_PAYMENT             |            10 |
| APP_4_VOIDSALESITEM       |            10 |
| APP_4_SALESCREDITNOTE     |             0 |
| APP_4_SALESCREDITNOTEITEM |             0 |
| APP_4_SALESDEBITNOTE      |             0 |
| APP_4_SALESDEBITNOTEITEM  |             0 |
| APP_4_EPAYMENTLOG         |             0 |
| APP_4_VOUCHER             |            10 |

**Reference Tables (Local):**

| Table                   | Rows Exported |
| ----------------------- | ------------: |
| APP_4_CASHIER_DRAWER    |            10 |
| APP_4_CUSTOMER          |            10 |
| APP_4_EXTENDEDSALESITEM |             0 |
| APP_4_ITEM              |            10 |
| APP_4_POINTRECORD       |            10 |
| APP_4_SALESDELIVERY     |             0 |
| APP_4_STOCK             |            10 |
| APP_4_VOUCHER_MASTER    |            10 |
| LOCATION_DETAIL         |            10 |

**Output Location:** `exports/sample_data/` (added to `.gitignore`)
