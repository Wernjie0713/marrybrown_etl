# 🧪 Local Testing Setup - FakeRestaurantDB

**Database:** FakeRestaurantDB (Local Testing)  
**Purpose:** Test API ETL pipeline safely before deploying to production

---

## Quick Setup

### Step 1: Update Database Scripts

Both SQL scripts need to use **FakeRestaurantDB** instead of MarryBrown_DW.

**File: `create_fact_table_api.sql`**

Change line 7 from:
```sql
USE MarryBrown_DW;
```

To:
```sql
USE FakeRestaurantDB;
```

**File: `schema_enhancements_for_api.sql`**

Change the first line from:
```sql
USE MarryBrown_DW;
```

To:
```sql
USE FakeRestaurantDB;
```

---

### Step 2: Create .env File for Local Testing

Create a new file: `.env.local` in `marrybrown_etl/` folder:

```env
# Local Testing Environment - FakeRestaurantDB
TARGET_DRIVER=ODBC Driver 17 for SQL Server
TARGET_SERVER=localhost
TARGET_DATABASE=FakeRestaurantDB
TARGET_USERNAME=sa
TARGET_PASSWORD=your_password_here

# Xilnex API (same for all environments)
XILNEX_API_HOST=api.xilnex.com
XILNEX_APP_ID=OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE
XILNEX_TOKEN=v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE=
XILNEX_AUTH_LEVEL=5
```

**Update your password** in the file above!

---

### Step 3: Run Database Setup

```bash
cd C:\Users\MIS INTERN\marrybrown_etl

# Option A: Using sqlcmd (Windows Authentication)
sqlcmd -S localhost -d FakeRestaurantDB -E -i create_fact_table_api.sql
sqlcmd -S localhost -d FakeRestaurantDB -E -i schema_enhancements_for_api.sql

# Option B: Using SQL Authentication
sqlcmd -S localhost -d FakeRestaurantDB -U sa -P your_password -i create_fact_table_api.sql
sqlcmd -S localhost -d FakeRestaurantDB -U sa -P your_password -i schema_enhancements_for_api.sql
```

**Verify tables created:**
```sql
USE FakeRestaurantDB;

SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%_api'
ORDER BY TABLE_NAME;

-- Expected results:
-- fact_sales_transactions_api
-- staging_payments_api
-- staging_sales_api
-- staging_sales_items_api
```

---

### Step 4: Update ETL Script to Use .env.local

**File: `api_etl/extract_from_api.py`**

At the top, change the environment file loading:

```python
# Load environment variables
load_dotenv('.env.local')  # ← Add .env.local for local testing
```

**File: `api_etl/transform_api_to_facts.py`**

Same change at the top:

```python
# Load environment variables
load_dotenv('.env.local')  # ← Add .env.local for local testing
```

---

### Step 5: Verify FakeRestaurantDB Has Dimension Tables

The API ETL needs these dimension tables to exist:

```sql
USE FakeRestaurantDB;

-- Check if dimension tables exist
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE 'dim_%'
ORDER BY TABLE_NAME;

-- Must have:
-- dim_date
-- dim_locations
-- dim_products
-- dim_staff
-- dim_payment_types
```

**If dimension tables are missing:**

```bash
# Run dimension ETL scripts first
cd C:\Users\MIS INTERN\marrybrown_etl

# Make sure .env.local points to FakeRestaurantDB
python generate_time_dims.py
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
```

---

## 🚀 Run Local Test

### Test 1: API ETL Pipeline

```bash
cd C:\Users\MIS INTERN\marrybrown_etl
venv\Scripts\activate

# Run API ETL (extracts October 2018)
python api_etl\run_api_etl_oct2018.py
```

**What to expect:**
- Calls Xilnex API repeatedly
- Extracts ~2,400 sales from October 2018
- Loads to FakeRestaurantDB staging tables
- Transforms to fact_sales_transactions_api
- Creates ~9,000 fact rows

**Verify data loaded:**

```sql
USE FakeRestaurantDB;

-- Check fact table
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT SaleNumber) as unique_sales,
    SUM(TotalAmount) as grand_total,
    MIN(DateKey) as min_date,
    MAX(DateKey) as max_date
FROM fact_sales_transactions_api;

-- Check new fields
SELECT TOP 10
    SaleNumber,
    TaxCode,
    TaxRate,
    IsFOC,
    Rounding,
    Model,
    IsServiceCharge
FROM fact_sales_transactions_api
WHERE TaxCode IS NOT NULL;
```

---

### Test 2: Backend API

**Update backend .env file:**

Create/Edit: `marrybrown_api/.env.local`

```env
# Local Testing - FakeRestaurantDB
TARGET_DRIVER=ODBC Driver 17 for SQL Server
TARGET_SERVER=localhost
TARGET_DATABASE=FakeRestaurantDB
TARGET_USERNAME=sa
TARGET_PASSWORD=your_password_here

# JWT Secret (same for all environments)
SECRET_KEY=your_secret_key_here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

**Start backend:**

```bash
cd C:\Users\MIS INTERN\marrybrown_api
venv\Scripts\activate

# Use .env.local for testing
# (You may need to update database.py to load .env.local)
uvicorn main:app --reload
```

**Test endpoints:**
1. Open: http://localhost:8000/docs
2. Look for "Sales Reports - API Test" section
3. Authorize with your token
4. Test: `POST /sales-api-test/reports/daily-sales`
   - Body: `{"start_date": "2018-10-01", "end_date": "2018-10-31", "location_key": null}`

---

### Test 3: Portal

**No changes needed** - portal talks to backend, backend talks to FakeRestaurantDB

```bash
cd C:\Users\MIS INTERN\marrybrown-portal
npm run dev
```

1. Open: http://localhost:5173
2. Login
3. Go to Reports → "🧪 Daily Sales (API Test)"
4. Set dates: Oct 1-31, 2018
5. Click "Run Report"
6. Should see data from FakeRestaurantDB!

---

## 🔍 Verification Checklist

- [ ] FakeRestaurantDB has fact_sales_transactions_api table
- [ ] FakeRestaurantDB has staging_*_api tables
- [ ] FakeRestaurantDB has all dimension tables (dim_date, dim_locations, etc.)
- [ ] .env.local created and points to FakeRestaurantDB
- [ ] API ETL runs successfully
- [ ] fact_sales_transactions_api has data (~9,000 rows)
- [ ] Backend connects to FakeRestaurantDB
- [ ] API test endpoints return data
- [ ] Portal displays data from FakeRestaurantDB
- [ ] Excel export works

---

## 🆘 Troubleshooting

### Database Connection Fails

**Error:** `Cannot open database "FakeRestaurantDB"`

**Solution:**
```sql
-- Check database exists
SELECT name FROM sys.databases WHERE name = 'FakeRestaurantDB';

-- If not exists, create it
CREATE DATABASE FakeRestaurantDB;
```

### Dimension Tables Missing

**Error:** `Invalid object name 'dim_locations'`

**Solution:** Run dimension ETL scripts first (see Step 5 above)

### Backend Can't Connect

**Check:** `marrybrown_api/database.py` is loading the right .env file

**Update database.py if needed:**
```python
from dotenv import load_dotenv
load_dotenv('.env.local')  # ← Add this for local testing
```

---

## ✅ Once Testing Passes

**If everything works in FakeRestaurantDB:**

1. **Document results** in test report
2. **Switch to production** (MarryBrown_DW):
   - Update SQL scripts: `USE MarryBrown_DW;`
   - Update .env to point to MarryBrown_DW
   - Re-run everything
3. **Deploy to cloud** (TIMEdotcom)

---

## 📊 Quick Validation Query

```sql
USE FakeRestaurantDB;

-- Complete validation report
SELECT 
    'fact_sales_transactions_api' as Table_Name,
    COUNT(*) as Row_Count,
    COUNT(DISTINCT SaleNumber) as Unique_Sales,
    COUNT(DISTINCT DateKey) as Unique_Dates,
    SUM(TotalAmount) as Total_Sales_Amount,
    MIN(DateKey) as Earliest_Date,
    MAX(DateKey) as Latest_Date,
    SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) as TaxCode_Populated,
    SUM(CASE WHEN IsFOC = 1 THEN 1 ELSE 0 END) as FOC_Items,
    SUM(CASE WHEN Rounding IS NOT NULL THEN 1 ELSE 0 END) as Rounding_Populated
FROM fact_sales_transactions_api;
```

---

**That's it! Test locally in FakeRestaurantDB first.** 🧪

Once everything works, switch to MarryBrown_DW for production testing!

