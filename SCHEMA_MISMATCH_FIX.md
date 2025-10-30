# Schema Mismatch Fix - Dimension Tables

**Date:** October 29, 2025  
**Issue:** `deploy_cloud_schema.sql` dimension table schemas don't match existing ETL scripts  
**Status:** ✅ FIXED - Use ETL Auto-Create Instead

---

## ❌ **The Problem**

Your `etl_dim_locations.py` failed with:
```
Invalid column name 'LocationGUID'
```

**Root Cause:**  
The `deploy_cloud_schema.sql` I created has **generic dimension table schemas** that **don't match** what your existing ETL scripts expect!

### **Example - dim_locations:**

**What deploy_cloud_schema.sql created:**
```sql
CREATE TABLE dim_locations (
    LocationKey INT IDENTITY PRIMARY KEY,
    LocationID NVARCHAR(50),     -- ❌ WRONG!
    LocationName NVARCHAR(200),
    Address NVARCHAR(500),       -- ❌ Extra column
    PostalCode NVARCHAR(20),     -- ❌ Extra column
    ...
)
```

**What etl_dim_locations.py expects:**
```sql
CREATE TABLE dim_locations (
    LocationKey INT IDENTITY PRIMARY KEY,
    LocationGUID NVARCHAR(50),   -- ✅ Correct!
    LocationName NVARCHAR(200),
    City NVARCHAR(100),
    State NVARCHAR(100),
    IsActive BIT
)
```

**Same problem exists for ALL dimension tables!**

---

## ✅ **The Solution: Let ETL Scripts Auto-Create Tables**

Your ETL scripts use pandas `.to_sql()` which **automatically creates tables** if they don't exist!

### **Steps to Fix:**

#### **1. Drop Existing Dimension Tables (if any exist)**

```powershell
sqlcmd -S 10.0.1.194,1433 -d MarryBrown_DW -U etl_user -P "ETL@MarryBrown2025!" -i drop_all_dim_tables.sql
```

This drops any dimension tables that were created with the wrong schema.

---

#### **2. Create Time Dimensions First** 

```powershell
python generate_time_dims.py
```

Creates `dim_date` and `dim_time` (these are correct).

---

#### **3. Run Dimension ETL Scripts** 

They will **auto-create tables** with the **correct schema** AND load data:

```powershell
python etl_dim_locations.py      # Auto-creates + loads ~310 locations
python etl_dim_products.py       # Auto-creates + loads ~1,500 products
python etl_dim_staff.py          # Auto-creates + loads ~500 staff
python etl_dim_payment_types.py  # Auto-creates + loads ~20 payment types
python etl_dim_customers.py      # Auto-creates + loads ~100 customers
python etl_dim_promotions.py     # Auto-creates + loads ~50 promotions
python etl_dim_terminals.py      # Auto-creates + loads ~100 terminals
```

**Each script will:**
1. Connect to cloud database
2. Check if table exists
3. If not, pandas creates it automatically with correct columns
4. Load the data

---

## 📊 **What About deploy_cloud_schema.sql?**

### **Option 1: Don't Use It for Dimensions** *(Recommended)*

- ✅ Use it ONLY for staging tables and fact tables
- ✅ Let ETL scripts create dimension tables

### **Option 2: Fix It Later** *(For future reference)*

I've updated `deploy_cloud_schema.sql` to fix `dim_locations`, but the other dim tables still need fixing. Since your ETL scripts will auto-create them correctly, it's easier to just use the ETL scripts.

---

## 🎯 **Revised Deployment Steps**

### **STEP 1: Deploy Only Staging & Fact Tables**

Edit `deploy_cloud_schema.sql` to comment out dimension table creation (or just run these parts manually):

```powershell
# Run only PART 2 (Staging) and PART 3 (Fact) from deploy_cloud_schema.sql
# Skip PART 1 (Dimensions) - let ETL create them
```

**OR** just create staging and fact tables manually (skip dimensions).

---

### **STEP 2: Generate Time Dimensions**

```powershell
python generate_time_dims.py
```

Expected: ✅ 2,922 dates + 1,440 times

---

### **STEP 3: Run Dimension ETL Scripts**

```powershell
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
python etl_dim_customers.py
python etl_dim_promotions.py
python etl_dim_terminals.py
```

**Expected:** Each script creates its table + loads data automatically!

---

### **STEP 4: Manually Create Staging & Fact Tables**

```sql
-- Create staging tables (from deploy_cloud_schema.sql PART 2)
CREATE TABLE staging_sales (...);
CREATE TABLE staging_sales_items (...);
CREATE TABLE staging_payments (...);
CREATE TABLE api_sync_metadata (...);

-- Create fact table (from deploy_cloud_schema.sql PART 3)
CREATE TABLE fact_sales_transactions (...);
```

---

### **STEP 5: Run API ETL**

```powershell
python api_etl\run_cloud_etl_multi_month.py
```

---

## 📝 **Summary**

**Problem:** Schema mismatch between `deploy_cloud_schema.sql` and existing ETL scripts  
**Solution:** Let ETL scripts auto-create dimension tables (pandas does this automatically)  
**Result:** Correct schemas + data loaded in one step!

---

## ⚡ **Quick Fix Commands**

```powershell
# 1. Drop wrong tables (if they exist)
sqlcmd -S 10.0.1.194,1433 -d MarryBrown_DW -U etl_user -P "ETL@MarryBrown2025!" -i drop_all_dim_tables.sql

# 2. Generate time dimensions
python generate_time_dims.py

# 3. Run ALL dimension ETL scripts
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
python etl_dim_customers.py
python etl_dim_promotions.py
python etl_dim_terminals.py

# Done! Now you have all dimensions with correct schemas and data!
```

---

**Status:** Ready to deploy! 🚀

