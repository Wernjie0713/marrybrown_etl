# Cloud Migration Summary
## API ETL to TIMEdotcom Cloud Warehouse

**Date:** October 29, 2025  
**Author:** YONG WERN JIE  
**Status:** ✅ Ready for Deployment

---

## 🎯 What We're Doing

**Migrating from Local Testing → Cloud Production**

We successfully validated the API ETL approach locally with `FakeRestaurantDB` (October 2018 data). Now we're deploying to the TIMEdotcom cloud warehouse to:

1. **Extract 15 months of historical data** (Oct 2018 - Dec 2019)
2. **Use API ETL exclusively** (not Direct DB)
3. **Store in production schema** (`fact_sales_transactions` not `_api`)
4. **Enable cloud-based reporting** (FastAPI + React Portal)

---

## 📁 What's Been Created

### Configuration Files

| File | Purpose | Contents |
|------|---------|----------|
| `.env.cloud` | Cloud DB credentials | Server: `10.0.1.194,1433`<br>Database: `MarryBrown_DW`<br>User: `etl_user` |
| `deploy_cloud_schema.sql` | Schema deployment | All dimension, staging, fact tables |
| `CLOUD_DEPLOYMENT_GUIDE.md` | Detailed instructions | Step-by-step deployment process |
| `CLOUD_DEPLOYMENT_CHECKLIST.md` | Quick checklist | Simplified task list |

### ETL Scripts (Updated for Cloud)

| Script | Changes Made | Purpose |
|--------|--------------|---------|
| `api_etl/extract_from_api.py` | - Uses `.env.cloud`<br>- Removed `_api` suffix<br>- Added generic `extract_sales_for_period()` | Extract sales from API for any date range |
| `api_etl/transform_api_to_facts.py` | - Uses `.env.cloud`<br>- Removed `_api` suffix<br>- Targets `fact_sales_transactions` | Transform staging → fact table |
| `api_etl/run_cloud_etl_multi_month.py` | **NEW**: 15-month orchestrator | Extracts Oct 2018 - Dec 2019 sequentially |

### Database Schema

**Dimension Tables** (8 total):
- `dim_date`, `dim_time`, `dim_locations`, `dim_products`
- `dim_customers`, `dim_staff`, `dim_payment_types`, `dim_promotions`, `dim_terminals`

**Staging Tables** (3 total, no `_api` suffix):
- `staging_sales` (21 columns)
- `staging_sales_items` (24 columns)
- `staging_payments` (14 columns)

**Fact Table** (1, unified name):
- `fact_sales_transactions` with 6 new API-specific fields:
  - `TaxCode`, `TaxRate`, `IsFOC`, `Rounding`, `Model`, `IsServiceCharge`

**Metadata & Utility**:
- `api_sync_metadata` (tracks API extraction progress)
- `vw_data_quality_check` (monitoring view)

---

## 🔄 Key Architectural Changes

### Before (Local Testing)
```
Xilnex Sync API
    ↓
Extract (October 2018 only)
    ↓
staging_sales_api, staging_sales_items_api, staging_payments_api
    ↓
Transform
    ↓
fact_sales_transactions_api
    ↓
FastAPI (localhost) → Portal (localhost)
```

### After (Cloud Production)
```
Xilnex Sync API
    ↓
Extract (Oct 2018 - Dec 2019, 15 months)
    ↓
staging_sales, staging_sales_items, staging_payments
    ↓
Transform (with dimension lookups)
    ↓
fact_sales_transactions (PRODUCTION TABLE)
    ↓
FastAPI (cloud) → Portal (cloud/local)
```

---

## 🚀 Deployment Process

### Phase 1: Schema Deployment (READY NOW)
```powershell
sqlcmd -S 10.0.1.194,1433 -d MarryBrown_DW -U etl_user -P "ETL@MarryBrown2025!" -i deploy_cloud_schema.sql
```

**Creates:**
- 8 dimension tables
- 3 staging tables
- 1 fact table with 26 columns + 6 new API fields
- 1 metadata table
- 1 data quality view
- 5 indexes for performance

---

### Phase 2: Dimension Population (MANUAL STEP)

**Options:**

**A. From Xilnex (Recommended)**
```powershell
# Update each script to use .env.cloud
python etl_dim_locations.py
python etl_dim_products.py
python etl_dim_staff.py
python etl_dim_payment_types.py
python generate_time_dims.py
```

**B. Quick Test with Minimal Data**
```sql
-- Insert test records to unblock testing
INSERT INTO dim_locations (LocationID, LocationName, ...) VALUES ...
```

---

### Phase 3: API ETL Execution (READY NOW)

```powershell
python api_etl\run_cloud_etl_multi_month.py
```

**What happens:**
1. Loops through 15 months (Oct 2018 - Dec 2019)
2. For each month:
   - Calls Xilnex Sync API with pagination
   - Filters sales by date range
   - Saves raw JSON to `api_data/raw_sales_{month}_{timestamp}.json`
   - Loads to staging tables
3. After all months extracted:
   - Transforms staging → `fact_sales_transactions`
   - Applies split-tender allocation logic
   - Performs dimension lookups
   - Populates new API fields

**Expected Runtime:** 30-75 minutes  
**Expected Output:** 2M+ fact records

---

### Phase 4: Backend/Portal Connection (MANUAL UPDATE)

Update `marrybrown_api/.env`:
```env
TARGET_SERVER=10.0.1.194,1433
TARGET_DATABASE=MarryBrown_DW
```

**Then test:**
```powershell
cd marrybrown_api
uvicorn main:app --reload
```

Visit: `http://localhost:8000/docs` → Test `/sales/reports/daily-sales`

---

## 📊 What's Different from Local Testing

| Aspect | Local Testing | Cloud Production |
|--------|---------------|------------------|
| **Environment** | `.env.local` | `.env.cloud` |
| **Server** | `localhost` | `10.0.1.194,1433` (VPN required) |
| **Database** | `FakeRestaurantDB` | `MarryBrown_DW` |
| **Table Names** | `*_api` suffix | Standard names (no suffix) |
| **Data Range** | Oct 2018 (1 month, 856 sales) | Oct 2018 - Dec 2019 (15 months, ~125K sales) |
| **Purpose** | Validation & testing | Production warehouse |
| **Fact Records** | ~17K rows | ~2.5M rows (estimated) |
| **Accuracy** | 99.98% vs Xilnex | Target: 99.5%+ |

---

## ✅ Validation & Testing

### Data Quality Checks

```sql
-- 1. Record counts
SELECT COUNT(*) FROM fact_sales_transactions;
-- Expected: 2M+ rows

-- 2. Date coverage
SELECT 
    LEFT(CAST(DateKey AS VARCHAR), 6) as YearMonth,
    COUNT(*) as Transactions,
    SUM(TotalAmount) as Sales
FROM fact_sales_transactions
GROUP BY LEFT(CAST(DateKey AS VARCHAR), 6)
ORDER BY YearMonth;
-- Expected: 15 rows (201810 to 201912)

-- 3. New API fields
SELECT 
    COUNT(*) as Total,
    SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) as WithTaxCode,
    SUM(CASE WHEN IsFOC = 1 THEN 1 ELSE 0 END) as FOCItems,
    SUM(CASE WHEN Model IS NOT NULL THEN 1 ELSE 0 END) as WithModel
FROM fact_sales_transactions;
-- Expected: 90%+ TaxCode, 1-5% FOC, 80%+ Model

-- 4. Use built-in view
SELECT * FROM vw_data_quality_check;
```

### Report Testing

1. **Daily Sales Report** (Oct 2018 - Dec 2019)
   - Portal endpoint: `/reports/daily-sales`
   - Export to Excel
   - Compare with Xilnex portal export
   - Target: 99.5%+ accuracy

2. **EOD Summary Report**
   - Verify split-tender allocation works
   - Check e-wallet breakdowns
   - Validate return transactions

3. **Product Mix Report**
   - Store-level analysis
   - Category breakdowns
   - Verify quantities and amounts

---

## 🎯 Success Criteria

- [x] Schema deployment script created and tested
- [x] API ETL scripts updated for cloud
- [x] Multi-month orchestrator implemented
- [x] Documentation complete (guides + checklists)
- [ ] Schema deployed to cloud *(awaiting execution)*
- [ ] Dimensions populated *(awaiting execution)*
- [ ] 15 months of data extracted *(awaiting execution)*
- [ ] Data quality validated *(awaiting execution)*
- [ ] Backend connected to cloud *(awaiting configuration)*
- [ ] Portal tested with cloud data *(awaiting deployment)*

---

## 🚧 Known Limitations & Future Work

### Current Limitations
1. **Dimension tables must be populated manually** before running API ETL
   - Future: Automate dimension ETL as part of cloud deployment

2. **No incremental sync yet** (full reload every time)
   - Future: Implement daily incremental using `lastTimestamp`

3. **Single-threaded extraction** (one month at a time)
   - Future: Parallel extraction for faster runtime

### Future Enhancements
1. **Scheduled ETL Jobs**
   - Windows Task Scheduler or cron job
   - Daily sync at 2 AM
   - Email notifications on failure

2. **Performance Optimization**
   - Partition fact table by year/month
   - Add covering indexes for common queries
   - Implement materialized views for reports

3. **Monitoring & Alerting**
   - API call tracking
   - Data quality alerts
   - ETL failure notifications

---

## 📝 Files Reference

### Essential Files for Deployment
```
marrybrown_etl/
├── .env.cloud                              # Cloud credentials ✅
├── deploy_cloud_schema.sql                 # Schema deployment ✅
├── CLOUD_DEPLOYMENT_GUIDE.md               # Detailed guide ✅
├── CLOUD_DEPLOYMENT_CHECKLIST.md           # Quick checklist ✅
├── api_etl/
│   ├── extract_from_api.py                 # Updated for cloud ✅
│   ├── transform_api_to_facts.py           # Updated for cloud ✅
│   └── run_cloud_etl_multi_month.py        # New orchestrator ✅
├── test_cloud_quick.py                     # Connection test ✅
└── test_cloud_connection.py                # Detailed test ✅
```

### Legacy Files (Keep for Reference)
```
├── .env.local                              # Local testing
├── create_fact_table_api.sql               # Local testing
├── schema_enhancements_for_api.sql         # Local testing
├── LOCAL_TEST_SETUP.md                     # Local testing guide
└── QUICKSTART_LOCAL_TEST.md                # Local quick guide
```

---

## 🎬 Next Steps (In Order)

### Immediate (Today)
1. ✅ Review this summary
2. ✅ Understand architectural changes
3. ⏳ Deploy schema to cloud (`deploy_cloud_schema.sql`)
4. ⏳ Populate dimension tables (manual or ETL scripts)
5. ⏳ Run multi-month API ETL (`run_cloud_etl_multi_month.py`)

### Short-term (This Week)
6. ⏳ Validate data quality (SQL queries)
7. ⏳ Update FastAPI backend configuration
8. ⏳ Test reports via local portal
9. ⏳ Compare with Xilnex portal exports

### Medium-term (Next Week)
10. ⏳ Deploy FastAPI to cloud Linux VM
11. ⏳ Deploy React portal to cloud
12. ⏳ Conduct User Acceptance Testing (UAT)
13. ⏳ Implement incremental sync

---

## 💡 Key Takeaways

1. **API ETL is production-ready** with 99.98% validation accuracy
2. **All scripts updated** to use `.env.cloud` and remove `_api` suffix
3. **15-month extraction** automated via `run_cloud_etl_multi_month.py`
4. **Schema deployment** is a one-command operation
5. **Dimension tables** are the only manual prerequisite

**Everything is ready for cloud deployment! 🚀**

---

**Questions or Issues?**
- Refer to `CLOUD_DEPLOYMENT_GUIDE.md` for detailed steps
- Use `CLOUD_DEPLOYMENT_CHECKLIST.md` for quick reference
- Check troubleshooting sections in both guides

**Ready to deploy? Start with the checklist!**

