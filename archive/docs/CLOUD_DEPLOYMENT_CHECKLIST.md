# Cloud Deployment Checklist
## Quick Reference for TIMEdotcom Cloud ETL Setup

**Date:** October 29, 2025  
**Target:** Deploy API ETL to cloud warehouse (`10.0.1.194`)

---

## ✅ Pre-Deployment Checklist

- [x] VPN connected to TIMEdotcom network
- [x] SQL Server installed on cloud (10.0.1.194)
- [x] Mixed Mode authentication enabled
- [x] `MarryBrown_DW` database created
- [x] `etl_user` login created with db_owner role
- [x] Tested connection with `test_cloud_quick.py`
- [ ] Xilnex database accessible for dimension ETL
- [ ] Python environment with required packages installed

---

## 📦 Step-by-Step Deployment

### ☐ STEP 1: Deploy Schema (5 minutes)

```powershell
cd "C:\Users\MIS INTERN\marrybrown_etl"
sqlcmd -S 10.0.1.194,1433 -d MarryBrown_DW -U etl_user -P "ETL@MarryBrown2025!" -i deploy_cloud_schema.sql
```

**Verify:** 18 tables created (8 dim, 3 staging, 1 fact, 1 metadata, 2 time, 3 utility)

---

### ☐ STEP 2: Populate Dimensions (15-30 minutes)

**Option A: From Xilnex (Recommended)**

```powershell
# Update each ETL script to use .env.cloud, then run:
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
python generate_time_dims.py  # For dim_date and dim_time
```

**Option B: Quick Test (Minimal Data)**

```sql
-- Insert test dimension records manually
-- See deploy_cloud_schema.sql comments for examples
```

**Verify Dimensions:**

```sql
SELECT 'dim_locations' as TableName, COUNT(*) as RowCount FROM dim_locations
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_staff', COUNT(*) FROM dim_staff
UNION ALL
SELECT 'dim_payment_types', COUNT(*) FROM dim_payment_types;
```

Expected: At least 50+ locations, 1000+ products, 100+ staff

---

### ☐ STEP 3: Test Connection (2 minutes)

```powershell
python test_cloud_quick.py
```

**Expected:** Connection successful, tables listed

---

### ☐ STEP 4: Run API ETL (30-75 minutes)

```powershell
# Full extraction: Oct 2018 - Dec 2019
python api_etl\run_cloud_etl_multi_month.py
```

**Monitor Progress:**
- Watch console for month-by-month extraction
- Check `api_data/` folder for raw JSON files
- Total time: ~60 minutes for 15 months

**Alternative: Single Month Test**

```powershell
# Test with just October 2018 first (faster)
python api_etl\run_api_etl_oct2018.py
```

---

### ☐ STEP 5: Validate Data (5 minutes)

```sql
USE MarryBrown_DW;

-- 1. Check fact table row count
SELECT COUNT(*) FROM fact_sales_transactions;
-- Expected: 2M+ rows for 15 months

-- 2. Check date coverage
SELECT 
    LEFT(CAST(DateKey AS VARCHAR), 6) as YearMonth,
    COUNT(*) as Rows
FROM fact_sales_transactions
GROUP BY LEFT(CAST(DateKey AS VARCHAR), 6)
ORDER BY YearMonth;
-- Expected: 15 rows (Oct 2018 - Dec 2019)

-- 3. Check new API fields
SELECT 
    COUNT(*) as Total,
    SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) as WithTaxCode,
    SUM(CASE WHEN IsFOC = 1 THEN 1 ELSE 0 END) as FOCItems
FROM fact_sales_transactions;
-- Expected: TaxCode 90%+, FOC 1-5%

-- 4. View data quality summary
SELECT * FROM vw_data_quality_check;
```

---

### ☐ STEP 6: Update Backend API (10 minutes)

**Edit `marrybrown_api/.env`:**

```env
TARGET_SERVER=10.0.1.194,1433
TARGET_DATABASE=MarryBrown_DW
TARGET_USERNAME=etl_user
TARGET_PASSWORD=ETL@MarryBrown2025!
```

**Test locally:**

```powershell
cd "C:\Users\MIS INTERN\marrybrown_api"
uvicorn main:app --reload
```

Visit: `http://localhost:8000/docs`

Test endpoint: `/sales/reports/daily-sales`
- Date Range: 2018-10-01 to 2018-10-31
- Expected: Data returns successfully

---

### ☐ STEP 7: Test Portal (Optional for now)

```powershell
cd "C:\Users\MIS INTERN\marrybrown-portal"
npm run dev
```

Visit: `http://localhost:5173`
- Login with test credentials
- Navigate to Daily Sales Report
- Select October 2018
- Export to Excel
- Compare with Xilnex portal export

---

## 🎯 Deployment Verification Matrix

| Component | Status | Verified By |
|-----------|--------|-------------|
| Cloud SQL Server | ✅ Connected | `test_cloud_quick.py` |
| Database Schema | [ ] Deployed | `deploy_cloud_schema.sql` |
| Dimension Tables | [ ] Populated | SQL COUNT queries |
| API ETL Extraction | [ ] Complete | Console output |
| Fact Table Data | [ ] Validated | `vw_data_quality_check` |
| FastAPI Backend | [ ] Connected | `/docs` endpoint |
| React Portal | [ ] Working | Manual testing |
| Data Accuracy | [ ] 99.5%+ | Comparison script |

---

## 🚨 Common Issues & Quick Fixes

### Issue: "Login failed for user 'etl_user'"
**Fix:** 
```powershell
# Verify VPN connected
# Check password in .env.cloud
# Re-run fix_etl_user.sql if needed
```

### Issue: "Invalid object name 'dbo.dim_locations'"
**Fix:**
```powershell
# Populate dimensions first (Step 2)
python etl_dim_locations.py
```

### Issue: "API returns 0 sales"
**Fix:**
```powershell
# Check date range in config_api.py
# Verify Xilnex API credentials
# Check api_data/ for raw JSON files
```

### Issue: "Transformation fails with 0 rows"
**Fix:**
```sql
-- Check if staging tables have data
SELECT COUNT(*) FROM staging_sales;
SELECT COUNT(*) FROM staging_sales_items;
SELECT COUNT(*) FROM staging_payments;

-- If empty, re-run extraction
```

---

## 📊 Success Metrics

After deployment, verify:

- ✅ **Data Completeness:** All 15 months present (Oct 2018 - Dec 2019)
- ✅ **Data Quality:** 99.5%+ match with Xilnex portal
- ✅ **API Fields:** TaxCode, Model, IsFOC populated
- ✅ **Performance:** Reports load in < 5 seconds
- ✅ **Export:** Excel export works correctly
- ✅ **Accuracy:** Profit calculations match Xilnex

---

## 🎬 Next Steps After Deployment

1. **Incremental ETL Setup**
   - Schedule daily API sync
   - Use `lastTimestamp` from metadata
   - Automate at 2 AM daily

2. **Portal Deployment**
   - Build production bundle
   - Deploy to TIMEdotcom Linux VM
   - Configure HTTPS

3. **User Acceptance Testing**
   - Share with stakeholders
   - Gather feedback
   - Iterate on reports

4. **Documentation**
   - Update Notion with deployment notes
   - Record FYP progress
   - Prepare presentation materials

---

## 📝 Deployment Log Template

```
DEPLOYMENT DATE: _______________
PERFORMED BY: YONG WERN JIE

[ ] Schema deployed at: __:__
[ ] Dimensions populated at: __:__
[ ] API ETL started at: __:__
[ ] API ETL completed at: __:__
[ ] Data validation passed at: __:__
[ ] Backend tested at: __:__
[ ] Portal tested at: __:__

TOTAL RECORDS: _______________
DATA ACCURACY: ___________%
ISSUES ENCOUNTERED: 
_________________________________
_________________________________

SIGN-OFF: _______________
```

---

**Ready to Deploy? Start with STEP 1! 🚀**

