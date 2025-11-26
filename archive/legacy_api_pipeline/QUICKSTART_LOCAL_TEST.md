# 🚀 Quick Start - Local Testing (FakeRestaurantDB)

**Time:** 10 minutes setup + 10 minutes testing  
**Database:** FakeRestaurantDB (Local)

---

## Step 1: Create .env.local (2 minutes)

```bash
cd C:\Users\MIS INTERN\marrybrown_etl

# Copy template
copy .env.local.template .env.local

# Edit .env.local and update:
# - TARGET_PASSWORD with your SQL Server password
```

**Your .env.local should look like:**
```env
TARGET_SERVER=localhost
TARGET_DATABASE=FakeRestaurantDB
TARGET_USERNAME=sa
TARGET_PASSWORD=YourActualPassword123
```

---

## Step 2: Run Database Setup (3 minutes)

```bash
# Create API test table in FakeRestaurantDB
sqlcmd -S localhost -d FakeRestaurantDB -E -i create_fact_table_api.sql

# If using SQL Auth instead:
# sqlcmd -S localhost -d FakeRestaurantDB -U sa -P YourPassword -i create_fact_table_api.sql
```

**Verify:**
```sql
USE FakeRestaurantDB;
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'fact_sales_transactions_api';
-- Should return 1
```

---

## Step 3: Check Dimension Tables Exist (1 minute)

```sql
USE FakeRestaurantDB;

SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE 'dim_%'
ORDER BY TABLE_NAME;

-- Must have: dim_date, dim_locations, dim_products, dim_staff, dim_payment_types
```

**If missing, run dimension ETL scripts first!**

---

## Step 4: Run API ETL (10 minutes)

```bash
cd C:\Users\MIS INTERN\marrybrown_etl
venv\Scripts\activate

# Run API ETL to extract October 2018 data
python api_etl\run_api_etl_oct2018.py
```

**Expected output:**
```
[Call 1] Fetching batch...
  Retrieved: 1000 sales
  Filtered: 234 sales in Oct 2018
  Total so far: 234 sales
...
EXTRACTION COMPLETE
  Sales Retrieved: 2,456

✓ Loaded 2,456 sales headers
✓ Loaded 8,234 sales items
✓ Loaded 2,678 payments

✓ Inserted 9,123 rows in 2.34 seconds

TRANSFORMATION COMPLETE!
```

---

## Step 5: Verify Data (1 minute)

```sql
USE FakeRestaurantDB;

SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT SaleNumber) as unique_sales,
    SUM(TotalAmount) as grand_total,
    MIN(DateKey) as min_date,
    MAX(DateKey) as max_date
FROM fact_sales_transactions_api;

-- Expected:
-- total_rows: ~9,000
-- unique_sales: ~2,400
-- grand_total: ~RM 500,000-800,000
-- min_date: 20181001
-- max_date: 20181031
```

**Check new fields:**
```sql
SELECT TOP 5
    SaleNumber,
    TaxCode,
    TaxRate,
    IsFOC,
    Rounding,
    Model
FROM fact_sales_transactions_api
WHERE TaxCode IS NOT NULL;
```

---

## Step 6: Test Backend (2 minutes)

```bash
# New terminal
cd C:\Users\MIS INTERN\marrybrown_api
venv\Scripts\activate
uvicorn main:app --reload
```

**Test in browser:**
1. Open: http://localhost:8000/docs
2. Look for "Sales Reports - API Test" section
3. Should see 3 endpoints

---

## Step 7: Test Portal (2 minutes)

```bash
# New terminal
cd C:\Users\MIS INTERN\marrybrown-portal
npm run dev
```

**Test in browser:**
1. Open: http://localhost:5173
2. Login
3. Reports → "🧪 Daily Sales (API Test)"
4. Dates: Oct 1-31, 2018
5. Click "Run Report"
6. Should see ~2,400 records!
7. Click "Export to Excel"

---

## ✅ Success!

If you got this far, your API ETL works in **FakeRestaurantDB**! 🎉

**Next steps:**
1. Export from Xilnex portal (Oct 2018)
2. Compare Excel files
3. Calculate accuracy
4. If ≥99.97%, ready for production!

---

## 🆘 Troubleshooting

**Error: Cannot open database 'FakeRestaurantDB'**
```sql
-- Check if exists
SELECT name FROM sys.databases WHERE name = 'FakeRestaurantDB';

-- If missing, check your actual database name
SELECT name FROM sys.databases;
```

**Error: Invalid object name 'dim_locations'**
```bash
# Run dimension ETL first
python generate_time_dims.py
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
```

**Error: API returns 401**
```bash
# Test API connection
python test_xilnex_sync_api.py
```

---

**That's it! Simple and fast.** 🚀

For detailed guide, see: `LOCAL_TEST_SETUP.md`

