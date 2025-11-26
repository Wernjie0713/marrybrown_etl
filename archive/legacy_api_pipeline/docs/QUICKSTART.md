# Quick Start Guide - ETL Pipeline

**Goal**: Get the ETL pipeline running in 10 minutes

---

## ⚡ Prerequisites

- ✅ Python 3.13+ installed
- ✅ Microsoft ODBC Driver 17 for SQL Server installed
- ✅ Access credentials for both Xilnex and Target databases

---

## 🚀 Setup (5 minutes)

### Step 1: Open Terminal

```bash
cd "C:\Users\MIS INTERN\marrybrown_etl"
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv
.\venv\Scripts\activate
```

You should see `(venv)` in your terminal prompt.

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

Wait for all packages to install (~2 minutes).

### Step 4: Configure Database Connections

Create a file named `.env` in this directory:

```env
# Source Database (Xilnex POS)
XILNEX_DRIVER=ODBC Driver 17 for SQL Server
XILNEX_SERVER=your_xilnex_server_here
XILNEX_DATABASE=your_database_name
XILNEX_USERNAME=your_username
XILNEX_PASSWORD=your_password

# Target Database (Cloud Warehouse)
TARGET_DRIVER=ODBC Driver 17 for SQL Server
TARGET_SERVER=your_target_server_here
TARGET_DATABASE=your_database_name
TARGET_USERNAME=your_username
TARGET_PASSWORD=your_password
```

**Replace** `your_*_here` with actual credentials.

### Step 5: Test Connections

```bash
python test_connections.py
```

**Expected Output**:
```
✅ Successfully connected to Xilnex source database!
✅ Successfully connected to new target database!
```

If you see ❌ errors, check your `.env` credentials.

---

## 📊 Running ETL (5 minutes)

### One-Time Setup: Load Time Dimensions

```bash
python generate_time_dims.py
```

This creates `dim_date` and `dim_time` tables. **Run once only.**

### Load All Dimension Tables

Run these commands in sequence:

```bash
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_customers.py
python etl_dim_staff.py
python etl_dim_promotions.py
python etl_dim_payment_types.py
```

Each script will show progress and completion message.

### Load Sales Fact Data

```bash
python etl_fact_sales_historical.py
```

This will:
- Extract sales data for the last 7 days
- Extract payment data from `APP_4_PAYMENT`
- Load into staging tables (`staging_sales`, `staging_sales_items`, `staging_payments`)
- Take ~5-10 minutes depending on data volume

**Note**: After this completes, you need to run the SQL transform script (see below).

---

## 🔄 Transform Sales Data (SQL)

After `etl_fact_sales_historical.py` completes:

1. Open SQL Server Management Studio (SSMS)
2. Connect to your target database
3. Open the file `transform_sales_facts.sql`
4. Execute the script (F5)

This transforms staging data into the final `fact_sales_transactions` table, including:
- Joining sales, sales items, and payment data
- Looking up surrogate keys from all dimension tables (including `dim_payment_types`)
- Calculating measures (GrossAmount, NetAmount, etc.)
- Populating `PaymentTypeKey` for payment method tracking

---

## ✅ Verify Success

### Check Row Counts

Open SSMS and run:

```sql
-- Check dimensions
SELECT 'dim_date' as table_name, COUNT(*) as rows FROM dim_date
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM dim_customers
UNION ALL
SELECT 'dim_locations', COUNT(*) FROM dim_locations
UNION ALL
SELECT 'dim_staff', COUNT(*) FROM dim_staff
UNION ALL
SELECT 'dim_promotions', COUNT(*) FROM dim_promotions
UNION ALL
SELECT 'dim_payment_types', COUNT(*) FROM dim_payment_types
UNION ALL
SELECT 'fact_sales_transactions', COUNT(*) FROM fact_sales_transactions;
```

**Expected Results**:
- `dim_date`: 4,018 rows
- `dim_time`: 86,400 rows
- `dim_products`: ~2,000 rows
- `dim_customers`: ~100,000 rows
- `dim_locations`: ~200 rows
- `dim_staff`: ~1,000 rows
- `dim_promotions`: ~500 rows
- `dim_payment_types`: ~10 rows
- `fact_sales_transactions`: Thousands to millions

### Test a Query

```sql
-- Total sales summary
SELECT 
    COUNT(DISTINCT SaleNumber) as total_transactions,
    SUM(NetAmount) as total_revenue,
    AVG(NetAmount) as avg_transaction_value
FROM fact_sales_transactions
WHERE SaleType = 'Sale';
```

### Test Payment Integration

```sql
-- Payment method breakdown
SELECT 
    pt.PaymentCategory,
    COUNT(DISTINCT f.SaleNumber) as transactions,
    SUM(f.NetAmount) as total_amount
FROM fact_sales_transactions f
LEFT JOIN dim_payment_types pt ON f.PaymentTypeKey = pt.PaymentTypeKey
WHERE f.SaleType = 'Sale'
GROUP BY pt.PaymentCategory
ORDER BY total_amount DESC;
```

If both queries return data with payment breakdown, **success!** 🎉

---

## 📅 Daily Updates

To update with new data:

### Option 1: Run All Dimensions + Facts

```bash
.\venv\Scripts\activate
python etl_dim_customers.py
python etl_dim_products.py
# ... other dimensions as needed
python etl_fact_sales_historical.py
# Then run transform_sales_facts.sql in SSMS
```

### Option 2: Facts Only (if dimensions unchanged)

```bash
.\venv\Scripts\activate
python etl_fact_sales_historical.py
# Then run transform_sales_facts.sql in SSMS
```

---

## 🔧 Common Issues

### ❌ `ModuleNotFoundError: No module named 'pandas'`

**Solution**: Virtual environment not activated or dependencies not installed

```bash
.\venv\Scripts\activate
pip install -r requirements.txt
```

### ❌ `Login timeout expired`

**Solution**: Check database server connectivity and credentials in `.env`

### ❌ `TRUNCATE TABLE failed`

**Solution**: Foreign key constraint issue. Manually delete data:

```sql
DELETE FROM [dbo].[table_name];
```

### ❌ Script hangs or timeouts during fact extraction

**Solution**: Reduce date range or chunk size in `etl_fact_sales_historical.py`:

```python
START_DATE = date.today() - timedelta(days=3)  # Reduce from 7 to 3
CHUNK_SIZE = 10000  # Reduce from 20000
```

---

## 📖 Next Steps

- **Understand the schema**: Read `DATABASE_SCHEMA.md`
- **Explore the data**: Run sample queries in SSMS
- **Check the API**: Go to `../marrybrown_api/` and follow its quickstart
- **Read full docs**: Check `README.md` for detailed information

---

## 🆘 Getting Help

1. **Check documentation**: `README.md` has detailed explanations
2. **Review logs**: Scripts print progress and errors
3. **Test connections**: Run `test_connections.py` first
4. **Contact team**: Reach out to MIS for database credentials or access issues

---

**Time to Complete**: 10-15 minutes  
**Last Updated**: October 2025

