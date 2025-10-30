# Cloud Deployment Guide
## Deploying API ETL to TIMEdotcom Cloud Warehouse

**Author:** YONG WERN JIE  
**Date:** October 29, 2025  
**Purpose:** Deploy API-based ETL to cloud for Oct 2018 - Dec 2019 data extraction

---

## 📋 Prerequisites

✅ **Already Completed:**
- [x] Cloud SQL Server installed on `10.0.1.194`
- [x] SQL Server configured with Mixed Mode authentication
- [x] VPN connection to cloud network established
- [x] `etl_user` created with `db_owner` role
- [x] Empty `MarryBrown_DW` database created

⚠️ **Still Required:**
- [ ] Xilnex database access (for dimension tables)
- [ ] Python 3.9+ with required libraries
- [ ] Active Xilnex Sync API credentials

---

## 🗂️ Files Created for Cloud Deployment

### Configuration Files
- `.env.cloud` - Cloud database credentials (TARGET_*)
- `deploy_cloud_schema.sql` - Complete schema deployment script

### ETL Scripts (Updated)
- `api_etl/extract_from_api.py` - Generic date-range extraction
- `api_etl/transform_api_to_facts.py` - Transform to fact_sales_transactions
- `api_etl/run_cloud_etl_multi_month.py` - **NEW**: 15-month orchestrator

### Key Changes from Local Testing
| Aspect | Local (FakeRestaurantDB) | Cloud (MarryBrown_DW) |
|--------|--------------------------|------------------------|
| Environment File | `.env.local` | `.env.cloud` |
| Server | `localhost` | `10.0.1.194,1433` |
| Database | `FakeRestaurantDB` | `MarryBrown_DW` |
| Fact Table | `fact_sales_transactions_api` | `fact_sales_transactions` |
| Staging Tables | `staging_*_api` | `staging_*` |
| Data Range | Oct 2018 (1 month) | Oct 2018 - Dec 2019 (15 months) |

---

## 🚀 Deployment Steps

### STEP 1: Deploy Database Schema

**On your local machine (connected via VPN):**

```powershell
# Navigate to ETL directory
cd "C:\Users\MIS INTERN\marrybrown_etl"

# Deploy schema using SQL authentication
sqlcmd -S 10.0.1.194,1433 -d MarryBrown_DW -U etl_user -P "ETL@MarryBrown2025!" -i deploy_cloud_schema.sql
```

**Expected Output:**
```
========================================
CLOUD WAREHOUSE SCHEMA DEPLOYMENT
========================================

PART 1: Creating Dimension Tables...
  [OK] Created dim_date
  [OK] Created dim_time
  [OK] Created dim_locations
  [OK] Created dim_products
  ... (8 dimension tables total)

PART 2: Creating Staging Tables...
  [OK] Created staging_sales
  [OK] Created staging_sales_items
  [OK] Created staging_payments

PART 3: Creating Fact Table...
  [OK] Created fact_sales_transactions with 6 new API fields
  [OK] Created performance indexes

PART 4: Creating Metadata Tables...
  [OK] Created api_sync_metadata

PART 5: Creating Data Quality View...
  [OK] Created vw_data_quality_check

========================================
CLOUD SCHEMA DEPLOYMENT COMPLETE!
========================================
```

**Verification:**
```sql
-- Run this to verify table creation
SELECT 
    TABLE_NAME, 
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;
```

---

### STEP 2: Populate Dimension Tables

**Option A: Copy from Local Warehouse** *(Fastest)*

If you have dimension data in `FakeRestaurantDB`:

```powershell
# Export from local, import to cloud
# (You'll need to write a transfer script or use SSIS/BCP)
```

**Option B: ETL from Xilnex** *(Most Accurate)*

```powershell
# Update existing dimension ETL scripts to use .env.cloud
# Then run each dimension ETL script:

python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
python etl_dim_customers.py
python etl_dim_promotions.py
python etl_dim_terminals.py
```

**Option C: Generate Time Dimensions** *(For dim_date and dim_time)*

```powershell
python generate_time_dims.py
```

**⚠️ IMPORTANT:** Dimension tables must be populated BEFORE running the fact table ETL, as the transformation script uses lookups to these tables.

---

### STEP 3: Test Connection to Cloud

```powershell
# Test Python connection
python test_cloud_quick.py
```

**Expected Output:**
```
============================================================
Quick Cloud SQL Server Connection Test
============================================================

Server: 10.0.1.194,1433
Database: MarryBrown_DW
Username: etl_user

WARNING: Make sure VPN is connected!

Connecting...
[OK] CONNECTION SUCCESSFUL!

Database: MarryBrown_DW
Tables found: 18

Sample tables:
  - dim_date
  - dim_locations
  - dim_products
  - fact_sales_transactions
  - staging_sales
  ...
```

---

### STEP 4: Run API ETL (Multi-Month Extraction)

**For the full 15-month extraction (Oct 2018 - Dec 2019):**

```powershell
# Make sure VPN is connected
# Activate virtual environment if using one
cd "C:\Users\MIS INTERN\marrybrown_etl"

# Run multi-month orchestrator
python api_etl\run_cloud_etl_multi_month.py
```

**What This Script Does:**
1. Extracts sales data from Xilnex Sync API month-by-month
2. Saves raw JSON for each month to `api_data/` directory
3. Loads data to staging tables (`staging_sales`, `staging_sales_items`, `staging_payments`)
4. After all months are extracted, transforms to `fact_sales_transactions`

**Expected Runtime:**
- ~2-5 minutes per month (depends on data volume)
- **Total: 30-75 minutes for 15 months**

**Progress Output:**
```
╔══════════════════════════════════════════════════════════════════════════════╗
║                    CLOUD ETL - MULTI-MONTH EXTRACTION                        ║
║                    October 2018 - December 2019                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Total months to extract: 15

[1/15] Processing October 2018...
================================================================================
EXTRACTING: October 2018
Date Range: 2018-10-01 to 2018-10-31
================================================================================

[Call 1] Fetching batch...
  Retrieved: 1000 sales
  Filtered: 856 sales in target range
  Total so far: 856 sales
  Next timestamp: 0x00000000A333D6F1

... (continues for each month)

================================================================================
EXTRACTION PHASE COMPLETE
================================================================================
  Successful Months: 15/15
  Total Sales Extracted: 125,483

================================================================================
STARTING TRANSFORMATION TO FACT TABLE
================================================================================

Step 1: Clearing existing fact_sales_transactions...
  [OK] Table cleared

Step 2: Transforming and loading data...
  [OK] Inserted 2,458,392 rows in 45.23 seconds

Step 3: Validating transformed data...
  Fact Records: 2,458,392
  Unique Sales: 125,483
  Total Amount: RM 45,678,901.25
  Date Range: 20181001 to 20191231

╔══════════════════════════════════════════════════════════════════════════════╗
║                              ETL COMPLETE!                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

  Total Time: 52.3 minutes
  Months Processed: 15
  Sales Extracted: 125,483
```

---

### STEP 5: Data Quality Verification

**Check data quality:**

```sql
-- Connect to cloud warehouse
USE MarryBrown_DW;

-- View data quality summary
SELECT * FROM dbo.vw_data_quality_check;

-- Verify fact table data
SELECT 
    COUNT(*) as TotalTransactions,
    COUNT(DISTINCT SaleNumber) as UniqueSales,
    SUM(TotalAmount) as TotalSales,
    MIN(DateKey) as FirstDate,
    MAX(DateKey) as LastDate
FROM dbo.fact_sales_transactions;

-- Check new API fields population
SELECT 
    COUNT(*) as TotalRows,
    SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) as TaxCodePopulated,
    SUM(CASE WHEN IsFOC = 1 THEN 1 ELSE 0 END) as FOCItems,
    SUM(CASE WHEN Model IS NOT NULL THEN 1 ELSE 0 END) as ModelPopulated,
    CAST(SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as TaxCodePercent
FROM dbo.fact_sales_transactions;

-- Verify data by month
SELECT 
    LEFT(CAST(DateKey AS VARCHAR), 6) as YearMonth,
    COUNT(*) as Transactions,
    SUM(TotalAmount) as Sales,
    COUNT(DISTINCT SaleNumber) as UniqueSales
FROM dbo.fact_sales_transactions
GROUP BY LEFT(CAST(DateKey AS VARCHAR), 6)
ORDER BY YearMonth;
```

**Expected Results:**
- TaxCode should be populated for 90%+ of transactions
- FOC items should be 1-5% of total
- Model should be populated for 80%+ of transactions
- All 15 months should have data (201810 through 201912)

---

### STEP 6: Update FastAPI to Connect to Cloud

**Edit `marrybrown_api/.env` or create `.env.cloud`:**

```env
# Cloud Warehouse Connection
TARGET_DRIVER=ODBC Driver 17 for SQL Server
TARGET_SERVER=10.0.1.194,1433
TARGET_DATABASE=MarryBrown_DW
TARGET_USERNAME=etl_user
TARGET_PASSWORD=ETL@MarryBrown2025!
```

**Update `marrybrown_api/database.py`** to load cloud credentials when deployed.

**Test API endpoints locally:**

```powershell
cd "C:\Users\MIS INTERN\marrybrown_api"
uvicorn main:app --reload
```

Then test: `http://localhost:8000/docs`

---

## 📊 Testing & Validation

### Test Scenarios

1. **Monthly Sales Report** (Oct 2018 - Dec 2019)
   - Portal: `/reports/daily-sales`
   - Date Range: 2018-10-01 to 2019-12-31
   - Export to Excel, verify totals

2. **Compare with Xilnex Portal**
   - Export same date range from Xilnex
   - Run comparison script:
     ```powershell
     python compare_completed_only.py
     ```
   - Target accuracy: 99.5%+

3. **New API Fields Validation**
   - Verify TaxCode, TaxRate, Model fields are populated
   - Check IsFOC flag for free items
   - Validate Rounding amounts

---

## 🔧 Troubleshooting

### Connection Issues

**Error: "Login failed for user 'etl_user'"**
- Verify VPN is connected
- Check password in `.env.cloud`
- Verify SQL Server allows SQL Server authentication (Mixed Mode)

**Error: "TCP Provider: The wait operation timed out"**
- Check VPN connection
- Verify private IP `10.0.1.194` is correct
- Test ping: `ping 10.0.1.194`
- Check SQL Server is running on cloud server

### ETL Issues

**Error: "Invalid object name 'dbo.dim_locations'"**
- Dimension tables not populated yet
- Run dimension ETL scripts first (Step 2)

**API Error: "401 Unauthorized"**
- Xilnex API credentials expired or incorrect
- Check APP_ID and TOKEN in `config_api.py`

**Transformation fails with "0 rows inserted"**
- Staging tables are empty
- Re-run extraction script
- Check date range in API response

### Data Quality Issues

**TaxCode population < 50%**
- Normal for older data (pre-GST)
- Acceptable if consistent with Xilnex portal

**Profit doesn't match Xilnex**
- Cost data may be missing for some products
- Check `dim_products` has cost information
- Verify item mapping (ProductCode to ProductKey)

---

## 📝 Next Steps After Deployment

1. ✅ **Deploy FastAPI to Cloud**
   - Set up Linux VM for FastAPI
   - Configure connection to cloud warehouse
   - Test all report endpoints

2. ✅ **Deploy React Portal**
   - Build production bundle
   - Deploy to Linux VM using Docker
   - Configure API endpoint URLs

3. ✅ **Set Up Incremental ETL**
   - Create scheduled task for daily API sync
   - Use `lastTimestamp` from `api_sync_metadata`
   - Automate dimension table updates

4. ✅ **Performance Optimization**
   - Add database indexes if needed
   - Implement query result caching
   - Optimize large date range queries

5. ✅ **User Acceptance Testing (UAT)**
   - Share portal URL with stakeholders
   - Gather feedback on reports
   - Identify missing features

---

## 🎯 Success Criteria

- [x] Schema deployed to cloud
- [ ] Dimension tables populated
- [ ] 15 months of fact data loaded (Oct 2018 - Dec 2019)
- [ ] Data quality checks pass (99.5%+ accuracy)
- [ ] FastAPI connects to cloud warehouse
- [ ] Portal displays reports correctly
- [ ] Export to Excel works
- [ ] Performance acceptable (< 5s for monthly reports)

---

## 📞 Support Contacts

- **Xilnex API Support:** (if API issues)
- **TIMEdotcom Cloud Support:** (if infrastructure issues)
- **Project Supervisor:** (for validation and approvals)

---

**Last Updated:** October 29, 2025  
**Status:** Ready for Deployment (Pending dimension table population)

