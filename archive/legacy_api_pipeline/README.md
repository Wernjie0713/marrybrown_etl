# Marrybrown ETL Pipeline

**Project**: Xilnex Data Liberation Initiative  
**Author**: YONG WERN JIE A22EC0121  
**Purpose**: Extract, Transform, and Load data from the Xilnex POS system into the new Marrybrown cloud data warehouse

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [ETL Scripts Documentation](#etl-scripts-documentation)
- [Parquet Export System](#parquet-export-system)
- [Database Schema](#database-schema)
- [Running the Pipeline](#running-the-pipeline)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Project Overview

This ETL pipeline is part of Marrybrown's data liberation initiative. The goal is to replicate and transform operational data from the third-party Xilnex POS system into our company-owned cloud data warehouse hosted by TIMEdotcom.

### Key Objectives

- **Data Independence**: Free Marrybrown's data from the restrictive Xilnex system
- **Analytics Optimization**: Transform OLTP data into an OLAP-optimized star schema
- **Data Quality**: Clean and standardize data during the transformation process
- **Continuous Replication**: Keep the data warehouse synchronized with the source system

### 🎯 Key Features (v1.7.0+)

- **✅ Split-Tender Payment Allocation**: Accurately handles invoices with multiple payment methods by proportionally allocating amounts across all payment types
- **✅ Accurate Tax Calculation**: Uses total tax (item + bill-level) for precise pre-tax revenue calculations
- **✅ SUBSALES_TYPE Support**: Classifies sales with secondary types (e.g., "Catering (Outdoor)")
- **✅ Multithreaded Processing**: Parallel extraction with ThreadPoolExecutor for faster ETL
- **✅ Chunked Loading**: Memory-efficient processing of large datasets (20K rows per chunk)
- **✅ Star Schema Design**: Optimized for analytics queries and BI reporting

### The Two-System Approach

- **System 1 (OLTP)**: Xilnex POS - Continues handling daily operations (read-only access)
- **System 2 (OLAP)**: New Cloud Database - Our analytics-optimized data warehouse (full control)

---

## 🏗️ Architecture

### ETL vs ELT Strategy

This project uses a **hybrid approach**:

#### **ETL Pattern** (for Dimension Tables)
- Small, slow-changing tables (customers, products, locations, etc.)
- Transform data in-memory using Python/Pandas
- Pattern: `Extract → Transform → Load`

#### **ELT Pattern** (for Fact Tables)
- Large, high-volume transactional tables (sales transactions)
- Transform data using SQL in the cloud database
- Pattern: `Extract → Load → Transform`
- Leverages the cloud database's massively parallel processing power

### Data Flow Diagram

```
┌─────────────────────┐
│  Xilnex POS (OLTP)  │
│   Source Database   │
└──────────┬──────────┘
           │ Extract (Python + SQLAlchemy)
           ▼
┌─────────────────────┐
│  Python ETL Scripts │
│  - Pandas Transform │
│  - Data Cleaning    │
│  - Chunked Reading  │
└──────────┬──────────┘
           │ Load
           ▼
┌─────────────────────┐
│  Staging Tables     │
│  (For Fact Data)    │
└──────────┬──────────┘
           │ Transform (SQL)
           ▼
┌─────────────────────┐
│  Star Schema Tables │
│  - Fact Tables      │
│  - Dimension Tables │
└─────────────────────┘
```

---

## 📁 Repository Layout (Nov 2025)

```
marrybrown_etl/
├── api_etl/             # API-first pipeline (chunked extraction codebase)
├── direct_db_etl/       # Legacy warehouse loaders for dims/facts
├── scripts/             # Utilities (migrations runner, exports, diagnostics)
├── tests/               # Connection and API smoke tests
├── migrations/          # Numbered SQL migrations (001–050)
├── archive/
│   ├── sql/             # Superseded schema tweaks
│   └── docs/            # Historical narratives & presentations
└── docs/                # Quickstart + schema references
```

Use `python scripts/run_migration.py all` to apply every migration in order, or pass a specific filename when needed.

---

## ♻️ Resume & Chunking Enhancements

- **`dbo.api_sync_metadata`** stores a single row per job (`JobName = sales_extraction:<start>:<end>`). Every chunk persists the latest `lastTimestamp`, status, row counts, and chunk duration so reruns can resume exactly where they stopped.
- **Adaptive chunk controller** automatically expands or shrinks chunk size between `CHUNK_MIN_SIZE`/`CHUNK_MAX_SIZE` to keep checkpoint durations steady (~`CHUNK_TARGET_SECONDS` seconds). Latency spikes trigger smaller batches; quiet periods allow larger ones.
- **Rate-limit aware retries** honor `Retry-After` headers and emit structured telemetry to `monitoring/metrics.log` (and optionally to Prometheus via `PROM_PUSHGATEWAY_URL` + `PROM_PUSH_JOB_NAME`).
- **Data-quality gates** run after each chunk and again after the SQL MERGE to ensure staging counts, unique keys, and revenue sums align before the data is promoted.

> **Key Environment Knobs**
>
> | Variable | Purpose | Default |
> | --- | --- | --- |
> | `CHUNK_MIN_SIZE` / `CHUNK_MAX_SIZE` | Bounds for adaptive chunk size (API calls per checkpoint) | 25 / 125 |
> | `CHUNK_TARGET_SECONDS` | Target duration for each chunk window | 180 |
> | `API_MAX_RETRIES` / `API_RETRY_BASE_DELAY` | Retry budget & base seconds for exponential backoff | 5 / 2 |
> | `STAGING_RETENTION_DAYS` | Days of staging data to keep after transform | 14 |
> | `METRICS_LOG_PATH`, `PROM_PUSHGATEWAY_URL`, `PROM_PUSH_JOB_NAME` | Metrics destinations | `monitoring/metrics.log`, unset, `marrybrown_etl` |
> | `WAREHOUSE_SESSION_CAP` | Maximum concurrent warehouse sessions | 8 |
> | `HISTORICAL_MAX_WORKERS` | Optional override for historical ELT threads | auto |

---

## 📦 Prerequisites

### Software Requirements

- Python 3.13+
- Microsoft ODBC Driver 17 for SQL Server
- Access to both source (Xilnex) and target (Cloud) databases

### Python Dependencies

```
pandas==2.3.3
numpy==2.3.3
pyodbc==5.2.0
sqlalchemy==2.0.43
python-dotenv==1.1.1
```

Install using:
```bash
pip install -r requirements.txt
```

---

## ⚙️ Setup Instructions

### 1. Clone or Navigate to the Project

```bash
cd "C:\Users\MIS INTERN\marrybrown_etl"
```

### 2. Create Virtual Environment

```bash
python -m venv venv
.\venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install pandas numpy pyodbc sqlalchemy python-dotenv
```

### 4. Configure Database Connections

Create a `.env` file in the project root:

```env
# Source Database (Xilnex POS)
XILNEX_DRIVER=ODBC Driver 17 for SQL Server
XILNEX_SERVER=your_xilnex_server
XILNEX_DATABASE=your_xilnex_database
XILNEX_USERNAME=your_username
XILNEX_PASSWORD=your_password

# Target Database (New Cloud Warehouse)
TARGET_DRIVER=ODBC Driver 17 for SQL Server
TARGET_SERVER=your_target_server
TARGET_DATABASE=your_target_database
TARGET_USERNAME=your_username
TARGET_PASSWORD=your_password

# Optional advanced tuning
# CHUNK_MIN_SIZE=25
# CHUNK_MAX_SIZE=125
# CHUNK_TARGET_SECONDS=180
# API_MAX_RETRIES=5
# API_RETRY_BASE_DELAY=2
# STAGING_RETENTION_DAYS=14
# METRICS_LOG_PATH=monitoring/metrics.log
# PROM_PUSHGATEWAY_URL=http://pushgateway:9091
# PROM_PUSH_JOB_NAME=marrybrown_etl
# WAREHOUSE_SESSION_CAP=8
# HISTORICAL_MAX_WORKERS=4
```

### 5. Test Connections

```bash
python tests/test_connections.py
```

Expected output:
```
✅ Successfully connected to Xilnex source database!
✅ Successfully connected to new target database!
```

---

## 📚 ETL Scripts Documentation

### Core Utility Functions

#### `get_db_engine(prefix)`
Reusable function to create SQLAlchemy database engines from `.env` credentials.

```python
source_engine = get_db_engine("XILNEX")
target_engine = get_db_engine("TARGET")
```

---

### Dimension ETL Scripts

All dimension scripts follow the same pattern:

1. **Extract**: Query source tables from Xilnex
2. **Transform**: Clean, rename, and standardize data with Pandas
3. **Load**: Truncate target table and append transformed data
4. **Default Records**: Insert "Unknown" records with `-1` surrogate keys

#### `scripts/generate_time_dims.py`

Generates time dimension tables programmatically (no source data needed).

- **`dim_date`**: One row per day from 2018-01-01 to 2028-12-31
  - `DateKey`: Integer in `YYYYMMDD` format (e.g., 20251007)
  - Includes: DayOfWeek, Month, Quarter, Year, IsWeekend flag

- **`dim_time`**: One row per second (86,400 rows)
  - `TimeKey`: Integer in `HHMMSS` format (e.g., 143000 for 2:30 PM)
  - Includes: Hour, Minute, Second, TimeOfDayBand

**Usage:**
```bash
python scripts/generate_time_dims.py
```bash
python scripts/generate_time_dims.py
```

---

#### `direct_db_etl/etl_dim_customers.py`

Extracts customer data from `APP_4_CUSTOMER` table.

**Source**: `COM_5013.APP_4_CUSTOMER`  
**Target**: `dbo.dim_customers`

**Key Transformations**:
- Combines `FIRST_NAME`, `LAST_NAME`, `COMPANY_NAME` into `FullName`
- Parses `FirstName` and `LastName` from combined name
- Converts invalid dates (e.g., `0001-01-01`) to `NULL`
- Inverts `BOOL_ACTIVE` to `IsActive` flag
- Inserts default "Unknown Customer" record with `CustomerKey = -1`

**Usage:**
```bash
python direct_db_etl/etl_dim_customers.py
```

---

#### `direct_db_etl/etl_dim_products.py`

Extracts product catalog from `APP_4_ITEM` table.

**Source**: `COM_5013.APP_4_ITEM`  
**Target**: `dbo.dim_products`

**Key Transformations**:
- Maps `BOOL_DISABLED` to `IsActive` (inverted logic: 0 → 1, 1 → 0)
- Identifies combo meals using `BOOL_ISPACKAGE` flag
- Removes duplicate `ProductCode` entries
- Preserves `SourceProductID` for traceability

**Usage:**
```bash
python direct_db_etl/etl_dim_products.py
```

---

#### `direct_db_etl/etl_dim_locations.py`

Extracts outlet/store location data.

**Source**: `COM_5013.LOCATION_DETAIL`  
**Target**: `dbo.dim_locations`

**Key Transformations**:
- Maps `LOCATION_DELETED` to `IsActive` flag (inverted)
- Sets `City` and `State` to `NULL` (address data not available in source DB)
- Note: Full address data must be manually supplemented from CSV export

**Usage:**
```bash
python direct_db_etl/etl_dim_locations.py
```

---

#### `direct_db_etl/etl_dim_staff.py`

Extracts staff/cashier data from sales transactions.

**Source**: `COM_5013.APP_4_SALES` (extracted distinct values)  
**Target**: `dbo.dim_staff`

**Key Transformations**:
- Extracts unique staff from `SALES_PERSON_USERNAME` and `CASHIER` columns
- Categorizes staff type:
  - "Integration" for Panda/Grab/FoodPanda
  - "System" for Kiosk
  - "Human" for regular staff
- Inserts default "Unspecified Staff" record with `StaffKey = -1`

**Usage:**
```bash
python direct_db_etl/etl_dim_staff.py
```

---

#### `direct_db_etl/etl_dim_promotions.py`

Extracts promotion and voucher data.

**Source**: `COM_5013.APP_4_VOUCHER_MASTER`  
**Target**: `dbo.dim_promotions`

**Key Transformations**:
- Maps `VOUCHER_MASTER_TYPE` numeric codes to descriptive strings
  - 1 → "Product Deal"
  - 2 → "Discount Voucher"
  - 4 → "Gift Voucher"
- Combines `IS_ACTIVATE` and `WBDELETED` into single `IsActive` flag
- Inserts default "No Promotion" record with `PromotionKey = -1`

**Usage:**
```bash
python direct_db_etl/etl_dim_promotions.py
```

---

## 📦 Parquet Export System

### Overview

The Parquet Export System provides client-side Python scripts to export sales data directly from Azure SQL Database to Parquet format for:
- **Bulk historical data migration testing**
- **Performance benchmarking**
- **Data archival and backup**
- **CDC strategy validation**

### Quick Start

```bash
# 1. Create .env file with Azure SQL credentials (see PARQUET_EXPORT_GUIDE.md)

# 2. Test connection
python test_azure_connection.py

# 3. Run export
python scripts/export_to_parquet.py

# 4. Validate results
python validate_parquet.py
```

### Performance Metrics

**October 2025 (14 days) - Actual Results:**
- **Sales**: 664,382 rows → 9.38 MB (47x compression) in 16.32s
- **Sales Items**: 3,487,947 rows → 44.60 MB (24x compression) in 81.74s
- **Payments**: 659,995 rows → 7.58 MB (18x compression) in 14.37s
- **Total**: 4.8M rows exported in under 2 minutes

### Key Features

✅ Direct Azure SQL → Parquet conversion (no intermediate files)  
✅ Chunked processing (50K rows/chunk) for memory efficiency  
✅ Snappy compression (3-5x reduction)  
✅ Progress tracking and validation  
✅ Environment-based configuration (`.env` support)

See **[PARQUET_EXPORT_GUIDE.md](PARQUET_EXPORT_GUIDE.md)** for complete documentation.

---

#### `direct_db_etl/etl_dim_payment_types.py`

Extracts distinct payment methods.

**Source**: `COM_5013.APP_4_PAYMENT`  
**Target**: `dbo.dim_payment_types`

**Key Transformations**:
- Extracts `DISTINCT` payment methods from `method` column
- Categorizes into high-level groups:
  - "Cash"
  - "Card"
  - "E-Wallet"
  - "Voucher"
  - "Other"

**Usage:**
```bash
python direct_db_etl/etl_dim_payment_types.py
```

---

### Fact Table ELT Script

#### `direct_db_etl/etl_fact_sales_historical.py`

Extracts sales transaction data using the **ELT pattern** with multithreading.

**Sources**: 
- `COM_5013.APP_4_SALES` → `staging_sales`
  - Includes SALES_TYPE (primary classification) and SUBSALES_TYPE (secondary classification for catering)
- `COM_5013.APP_4_SALESITEM` → `staging_sales_items`
  - Extracts `DOUBLE_TOTAL_TAX_AMOUNT` (total tax including item-level + bill-level allocation) used for accurate "Sales ex. Tax" calculations
  - Also extracts `double_mgst_tax_amount` (item-level tax only) for reference
- `COM_5013.APP_4_PAYMENT` → `staging_payments`

**Target**: `dbo.fact_sales_transactions` (via SQL transform)

**Configuration Variables**:
```python
START_DATE = date.today() - timedelta(days=7)  # 7 days ago
END_DATE = date.today() - timedelta(days=1)     # Yesterday
CHUNK_SIZE = 20000  # Process 20K rows at a time
MAX_WORKERS = 4     # Number of concurrent threads
```

**Process Flow**:

1. **Extract & Load** (Python):
   - Processes data day-by-day in configurable date range
   - Uses `pd.read_sql()` with `chunksize=20000` for memory efficiency
   - **Multithreading**: `ThreadPoolExecutor` processes multiple days in parallel
   - **Status Handling**: Extracts ALL transaction statuses (COMPLETED, CANCELLED, etc.) for maximum flexibility
   - **SUBSALES_TYPE**: Extracts secondary sale classification used to distinguish catering orders from regular take-away
   - Loads raw data into `staging_sales`, `staging_sales_items`, and `staging_payments`

2. **Transform** (SQL):
   - After extraction completes, run `transform_sales_facts.sql`
   - Joins staging tables with all dimension tables (including `dim_payment_types`)
   - Calculates measures (GrossAmount, DiscountAmount, NetAmount, etc.)
   - Populates `PaymentTypeKey` for payment method tracking
   - Maps `SubSalesType` to support detailed sales reporting (catering reclassification)
   - Uses `COALESCE` to default missing foreign keys to `-1`

**Usage**:
```bash
# Step 1: Extract and Load
python direct_db_etl/etl_fact_sales_historical.py

# Step 2: Transform (run SQL script in SSMS or via sqlcmd)
# Execute: transform_sales_facts.sql
```

**Performance Notes**:
- Processing ~7 days of data typically takes 5-10 minutes
- Adjust `MAX_WORKERS` based on your system (don't set too high)
- Increase `CHUNK_SIZE` if you have more memory available

---

### SQL Transform Script

#### `transform_sales_facts.sql`

SQL-based transformation that processes staged sales data into the final fact table.

**Input**: `staging_sales`, `staging_sales_items`, `staging_payments`  
**Output**: `fact_sales_transactions`

**Key Operations**:
- Joins staging tables with all 8 dimension tables
- Links payment data via `staging_payments` → `dim_payment_types`
- Converts `varchar` dates to integer `DateKey` (YYYYMMDD)
- Converts `varchar` times to integer `TimeKey` (HHMMSS)
- Populates `PaymentTypeKey` foreign key for payment method tracking
- Calculates derived measures:
  - `GrossAmount = price × quantity`
  - `NetAmount = GrossAmount - DiscountAmount`
  - `TotalAmount = NetAmount + TaxAmount`
  - `CostAmount = cost × quantity`
- Handles receipts with multiple payment rows by selecting a single representative payment per `invoice_id` (prioritizing non-empty card types). Additional split-tender detail is not yet surfaced in the fact table.
- Stores ALL transaction statuses (`SalesStatus` column) for flexible reporting. Status filtering is applied at the API/frontend level, not during ETL.

**Usage**:
Execute in SQL Server Management Studio or via command line:
```bash
sqlcmd -S your_server -d your_database -i transform_sales_facts.sql
```

---

## 🗄️ Database Schema

### Star Schema Overview

```
                    ┌──────────────┐
                    │   dim_date   │
                    └──────┬───────┘
                           │
    ┌──────────────┐       │       ┌──────────────┐
    │ dim_products │       │       │ dim_customers│
    └──────┬───────┘       │       └──────┬───────┘
           │               │              │
           │      ┌────────▼────────┐     │
           ├─────►│ fact_sales_     │◄────┤
           │      │  transactions   │     │
           │      └────────▲────────┘     │
           │               │              │
    ┌──────┴───────┐       │       ┌──────┴───────┐
    │ dim_locations│       │       │  dim_staff   │
    └──────────────┘       │       └──────────────┘
                           │
                    ┌──────┴───────┐
                    │  dim_time    │
                    └──────────────┘
```

### Table Descriptions

#### Fact Table

| Table | Purpose | Grain |
|-------|---------|-------|
| `fact_sales_transactions` | Sales line items | One row per item sold on a receipt |

#### Dimension Tables

| Table | Purpose | Rows (Approx) |
|-------|---------|---------------|
| `dim_date` | Calendar dates | 4,018 (2018-2028) |
| `dim_time` | Time of day | 86,400 (every second) |
| `dim_products` | Product catalog | ~2,000 |
| `dim_customers` | Loyalty members | ~100,000 |
| `dim_locations` | Store outlets | ~200 |
| `dim_staff` | Cashiers/systems | ~1,000 |
| `dim_promotions` | Vouchers/deals | ~500 |
| `dim_payment_types` | Payment methods | ~10 |

---

## 🚀 Running the Pipeline

### Full Historical Load Process

Follow this sequence for a complete data refresh:

```bash
# 1. Activate virtual environment
.\venv\Scripts\activate

# 2. Test database connections
python tests/test_connections.py

# 3. Load time dimensions (one-time setup)
python scripts/generate_time_dims.py

# 4. Load all dimension tables
python direct_db_etl/etl_dim_locations.py
python direct_db_etl/etl_dim_products.py
python direct_db_etl/etl_dim_customers.py
python direct_db_etl/etl_dim_staff.py
python direct_db_etl/etl_dim_promotions.py
python direct_db_etl/etl_dim_payment_types.py

# 5. Extract and load sales data (EL step)
python direct_db_etl/etl_fact_sales_historical.py

# 6. Transform sales data (T step - run in SSMS or via sqlcmd)
# Execute: transform_sales_facts.sql
```

### Incremental Update Process

For daily updates (after initial load):

```bash
# Update dimensions (if source data changed)
python direct_db_etl/etl_dim_products.py
python direct_db_etl/etl_dim_customers.py
# ... other dimensions as needed

# Load new sales data
# Edit START_DATE and END_DATE in direct_db_etl/etl_fact_sales_historical.py
python direct_db_etl/etl_fact_sales_historical.py

# Transform new data
# Execute: transform_sales_facts.sql
```

### Prefect Orchestration (optional but recommended)

The repository ships with `orchestration/prefect_pipeline.py`, a Prefect v3 flow that wires:

1. Dimension CDC loaders (skips work when hashes match)
2. Chunked API ETL (resume-aware, adaptive chunk controller)
3. Post-transform QA (staging vs fact check + staging retention cleanup)

Deploy it with:

```bash
prefect deploy orchestration/prefect_pipeline.py:marrybrown_flow \
  --name marrybrown-prod \
  --concurrency-limit 3 \
  --collision-strategy ENQUEUE
```

Trigger a run on demand:

```bash
prefect deployment run marrybrown-flow/marrybrown-prod \
  --param start_date=2018-10-01 \
  --param end_date=2018-12-31 \
  --param chunk_size=50 \
  --param resume=true
```

Use Prefect work pools/agents to pin the deployment to your preferred execution environment.

---

## 🔧 Troubleshooting

### Common Issues

#### ❌ `No module named 'dotenv'`

**Solution**: Install dependencies
```bash
pip install python-dotenv
```

#### ❌ `[Microsoft][ODBC Driver 17 for SQL Server]Login timeout expired`

**Solutions**:
1. Check database server is reachable
2. Verify credentials in `.env` file
3. Check firewall/VPN connection
4. Ensure SQL Server allows remote connections

#### ❌ `OperationalError: (pyodbc.OperationalError) ... SSL Provider`

**Solution**: Add `TrustServerCertificate=yes` to connection string (already included in scripts)

#### ❌ `TRUNCATE TABLE failed: foreign key constraint`

**Solution**: The table has foreign key references. Use this instead:
```sql
DELETE FROM [dbo].[table_name];
DBCC CHECKIDENT ('table_name', RESEED, 0);
```

#### ❌ `Timeout expired` during sales data extraction

**Solutions**:
1. Reduce date range (fewer days at a time)
2. Reduce `CHUNK_SIZE` (process fewer rows per batch)
3. Reduce `MAX_WORKERS` (fewer parallel threads)

#### ❌ Memory errors during data loading

**Solutions**:
1. Reduce `CHUNK_SIZE` to 10000 or 5000
2. Process smaller date ranges
3. Reduce `MAX_WORKERS`

---

## 📊 Performance Optimization

### Current Configuration

- **Date Range**: 7 days (configurable)
- **Chunk Size**: 20,000 rows
- **Max Workers**: 4 threads
- **Estimated Time**: 5-10 minutes for 7 days of data

### Tuning Recommendations

| Scenario | Chunk Size | Max Workers | Date Range |
|----------|------------|-------------|------------|
| Low Memory (8GB) | 5,000 | 2 | 3 days |
| Medium Memory (16GB) | 20,000 | 4 | 7 days |
| High Memory (32GB+) | 50,000 | 8 | 30 days |

### Database Connection Pooling

All scripts use `pool_pre_ping=True` to maintain connection health during long-running jobs.

---

## 📈 Monitoring & Metrics

- **Chunk metrics**: Every checkpoint appends a JSON line to `monitoring/metrics.log` (chunk number, duration, rows, retries, last timestamp). When `PROM_PUSHGATEWAY_URL` is set, the same metrics are pushed to the configured Prometheus gateway for Grafana dashboards.
- **Data-quality validation**: `monitoring/data_quality.py` enforces unique `SaleID` counts + staging vs fact revenue parity per date window. Failures raise `DataQualityError`, halting the run before data promotion.
- **Fact transforms**: `transform_api_to_facts.py` now calls the validator post-MERGE and purges staging partitions older than `STAGING_RETENTION_DAYS` to keep the footprint bounded.
- **Direct DB ELT**: `etl_fact_sales_historical.py` auto-tunes worker count using CPU/RAM heuristics (bounded by `WAREHOUSE_SESSION_CAP`) and reports per-day durations for easier throughput tracking.

---

## ⚠️ Known Data Quality Issues

### Cost Data Completeness

**Issue**: Some items in the Xilnex source (`APP_4_SALESITEM.double_cost`) have NULL or zero cost values.

**Impact**:
- Affects profit calculations in reports
- Approximately **20% of sales items** have missing cost data
- Estimated impact: ~RM 86 profit understatement per RM 3,000 in sales (~2.8%)
- **Example**: On 2025-10-10 at MB A FAMOSA, 108 out of 555 line items (19.5%) had NULL/zero costs

**Root Cause**: 
- Cost data is not consistently maintained in the Xilnex POS system
- Some products lack cost information in the master product database
- Zero-cost items (promotional items, add-ons, modifiers) are legitimately zero

**Current Behavior**:
- ETL pipeline extracts cost data "as-is" from source (NULL or zero values preserved)
- Profit calculations use available cost data: `Profit = NetAmount - CostAmount`
- Missing costs result in inflated profit calculations (cost is understated)

**Workarounds**:
- Use **Gross Profit Margin (GPM)** trends instead of absolute profit amounts
- Compare period-over-period changes rather than absolute values
- Focus on sales volume and revenue metrics which are accurate

**Recommendation**: 
Work with operations team to:
1. Ensure all products have cost values in Xilnex master data
2. Implement validation rules in POS to prevent NULL costs
3. Regular data quality audits to identify and fix missing costs


## 📝 Future Enhancements

### Planned Improvements

1. **Production Bulk Export**
   - Currently using Python extraction as temporary solution
   - Waiting for Xilnex admin to provide bulk CSV export capability
   - Will transition to high-speed `COPY INTO` pattern

2. **Change Data Capture (CDC)**
   - Real-time change tracking from Xilnex transaction log
   - Automated incremental updates
   - Apache Airflow orchestration

3. **Data Quality Checks**
   - Automated validation scripts
   - Orphan record detection
   - Data completeness reports
   - Cost data quality monitoring

4. **Error Handling & Logging**
   - Structured logging to files
   - Email alerts on failures
   - Retry logic for transient errors

---

## 📞 Support & Contact

**Project Owner**: YONG WERN JIE A22EC0121  
**Department**: MIS (Internship Project)  
**Documentation**: See Notion workspace for detailed analysis reports

For questions or issues, please contact the MIS team.

---

## 📄 License & Confidentiality

**Confidential**: This codebase contains proprietary Marrybrown business data and logic.  
**Internal Use Only**: Not for distribution outside the organization.

