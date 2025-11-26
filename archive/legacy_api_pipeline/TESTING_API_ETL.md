# API ETL Testing Guide

**Purpose:** Validate Xilnex Sync API ETL pipeline by comparing Excel exports with Xilnex portal

**Test Data:** October 2018 (first month returned by API)

**Success Criteria:** ≥99.97% financial accuracy match

---

## Prerequisites

1. ✅ Database schema enhanced (fact_sales_transactions_api created)
2. ✅ API credentials configured and working
3. ✅ FastAPI backend with test endpoints
4. ✅ Portal with API test page
5. ✅ Access to Xilnex portal for comparison export

---

## Step 1: Run Database Schema Setup

**Purpose:** Create fact_sales_transactions_api and staging tables

```bash
cd C:\Users\MIS INTERN\marrybrown_etl

# Run schema setup
sqlcmd -S your-server -d MarryBrown_DW -i create_fact_table_api.sql
sqlcmd -S your-server -d MarryBrown_DW -i schema_enhancements_for_api.sql
```

**Verify:**
```sql
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE '%_api';
-- Should return 4 tables: fact_sales_transactions_api, staging_sales_api, staging_sales_items_api, staging_payments_api
```

---

## Step 2: Run API ETL Pipeline

**Purpose:** Extract October 2018 data from Xilnex API and load to warehouse

```bash
cd C:\Users\MIS INTERN\marrybrown_etl

# Activate virtual environment
venv\Scripts\activate

# Run API ETL (this will take 5-10 minutes)
python api_etl\run_api_etl_oct2018.py
```

**What happens:**
1. Calls Xilnex Sync API repeatedly until October 2018 data retrieved
2. Saves raw JSON to `api_data/` folder
3. Loads to staging_sales_api, staging_sales_items_api, staging_payments_api
4. Transforms to fact_sales_transactions_api with split-tender logic

**Expected Output:**
```
[STEP 1/4] Extracting from Xilnex Sync API...
  Retrieved: 1000 sales
  Filtered: 234 sales in Oct 2018
  ...
[SUCCESS] Extracted 2,456 sales from API

[STEP 2/4] Saving raw JSON...
[SUCCESS] Saved to api_data/raw_sales_oct2018_...json

[STEP 3/4] Loading to staging tables...
  ✓ Loaded 2,456 sales headers
  ✓ Loaded 8,234 sales items
  ✓ Loaded 2,678 payments

[STEP 4/4] Transforming to fact_sales_transactions_api...
  ✓ Inserted 9,123 rows
  
ETL PIPELINE COMPLETE!
```

**Verify Data Loaded:**
```sql
-- Check fact table
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT SaleNumber) as unique_sales,
    SUM(TotalAmount) as grand_total,
    MIN(DateKey) as min_date,
    MAX(DateKey) as max_date
FROM dbo.fact_sales_transactions_api;

-- Expected:
-- total_rows: ~9,000-10,000
-- unique_sales: ~2,400-2,500
-- grand_total: ~RM 500,000 - 800,000
-- min_date: 20181001
-- max_date: 20181031

-- Check new fields populated
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) as tax_code_populated,
    SUM(CASE WHEN IsFOC = 1 THEN 1 ELSE 0 END) as foc_items,
    SUM(CASE WHEN Rounding IS NOT NULL THEN 1 ELSE 0 END) as rounding_populated
FROM dbo.fact_sales_transactions_api;
```

---

## Step 3: Start FastAPI Backend

**Purpose:** Start backend with API test endpoints

```bash
cd C:\Users\MIS INTERN\marrybrown_api

# Activate virtual environment
venv\Scripts\activate

# Start FastAPI
uvicorn main:app --reload
```

**Verify Endpoints:**
- Open browser: http://localhost:8000/docs
- Check for "Sales Reports - API Test" section
- Should see:
  - `GET /sales-api-test/daily-summary`
  - `POST /sales-api-test/reports/daily-sales`
  - `POST /sales-api-test/reports/daily-sales-detail`

**Test Endpoint Manually:**
```bash
# Try calling the API directly
curl -X POST "http://localhost:8000/sales-api-test/reports/daily-sales" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2018-10-01",
    "end_date": "2018-10-31",
    "location_key": null
  }'
```

---

## Step 4: Start Portal Frontend

**Purpose:** Access API test report page in browser

```bash
cd C:\Users\MIS INTERN\marrybrown-portal

# Install dependencies (if needed)
npm install

# Start Vite dev server
npm run dev
```

**Access Portal:**
1. Open browser: http://localhost:5173
2. Login with your credentials
3. Navigate to "Reports"
4. Click "🧪 Daily Sales (API Test)" card

---

## Step 5: Export from Portal

**Purpose:** Generate Excel export from API ETL data

**Steps:**
1. In the API Test report page, set date range:
   - Start Date: **October 1, 2018**
   - End Date: **October 31, 2018**
2. Click "Run Report"
3. Wait for data to load (should show ~2,400-2,500 records)
4. Click "Export to Excel" button
5. Save file as: `Daily_Sales_API_Test_20181001_20181031.xlsx`

**Verify Export:**
- Open Excel file
- Check columns: Date, Store Name, Sales Amount (RM), Profit Amount (RM)
- Check last row has "TOTAL" summary
- Note the totals for comparison

---

## Step 6: Export from Xilnex Portal

**Purpose:** Generate comparison Excel from Xilnex source system

**Steps:**
1. Login to Xilnex portal: https://portal.xilnex.com
2. Navigate to Reports → Daily Sales
3. Set filters:
   - **Date Range:** October 1-31, 2018
   - **All Outlets** (or filter specific outlets if needed)
   - **Status:** Completed
4. Click "Generate Report"
5. Export to Excel
6. Save as: `Xilnex_Daily_Sales_Oct2018.xlsx`

---

## Step 7: Compare Excel Files

**Purpose:** Validate API ETL accuracy against Xilnex portal

**Manual Comparison:**
1. Open both Excel files side-by-side
2. Compare key metrics:

| Metric | Your Portal (API ETL) | Xilnex Portal | Match? |
|--------|----------------------|---------------|--------|
| Total Sales Count | _________ | _________ | ⬜ |
| Total Sales Amount (RM) | _________ | _________ | ⬜ |
| Total Profit Amount (RM) | _________ | _________ | ⬜ |
| Outlet 1 Total | _________ | _________ | ⬜ |
| Outlet 2 Total | _________ | _________ | ⬜ |

**Calculate Accuracy:**
```
Accuracy % = (Your Portal Amount / Xilnex Portal Amount) × 100

Example:
Your Portal: RM 723,456.78
Xilnex Portal: RM 724,000.00
Accuracy: (723,456.78 / 724,000.00) × 100 = 99.92% ✅
```

**Success Criteria:**
- ✅ **≥99.97% accuracy** → API ETL is production-ready!
- ⚠️ **99.00-99.96% accuracy** → Investigate discrepancies (acceptable but needs review)
- ❌ **<99.00% accuracy** → Major issues, don't proceed to production

**Common Discrepancies:**
- Rounding differences (±RM 0.05 per sale)
- Return transaction handling differences
- Split-tender allocation differences
- Timing differences (business date vs system date)

---

## Step 8: Document Results

**Purpose:** Record findings for decision-making

**Create Test Results Document:**

```markdown
# API ETL Test Results - October 2018

**Test Date:** [Today's Date]
**Tester:** YONG WERN JIE

## Summary
- API ETL Status: ✅ Working / ❌ Failed
- Data Extracted: ____ sales from API
- Data Loaded: ____ rows to fact_sales_transactions_api
- Accuracy: _____% (vs Xilnex portal)

## Detailed Results

### Data Volume
- Sales in Portal: ____
- Sales in Xilnex: ____
- Match: ✅ / ❌

### Financial Totals
- Total Sales Amount (Portal): RM ______
- Total Sales Amount (Xilnex): RM ______
- Difference: RM ______ (_____%)

### New Fields Validation
- TaxCode Populated: _____% of records
- IsFOC Items Found: ____ items
- Rounding Data: ✅ Present / ❌ Missing

## Conclusion
[ ] ✅ API ETL is accurate enough for production
[ ] ⚠️ API ETL needs minor adjustments
[ ] ❌ API ETL has major issues, revert to Direct DB

## Next Steps
1. [ ] Review with supervisor
2. [ ] Decision: API vs Direct DB vs Hybrid
3. [ ] Deploy to cloud (if approved)
```

---

## Troubleshooting

### Issue: API Returns 401 Unauthorized
**Solution:** Check if API token is still enabled in Xilnex admin panel

### Issue: No Data in fact_sales_transactions_api
**Solution:** Check ETL logs for errors, verify staging tables have data

### Issue: Portal Shows "No data found"
**Solution:** 
- Check date range (must be October 2018)
- Verify FastAPI backend is running
- Check browser console for API errors

### Issue: Excel Export is Empty
**Solution:** 
- Ensure you clicked "Run Report" first
- Check that data loaded in table before exporting

### Issue: Xilnex Portal Can't Filter to Oct 2018
**Solution:** 
- Xilnex might not have historical data access
- Use recent data instead (e.g., current month)
- Update test to use recent dates

---

## Quick Reference: File Locations

**ETL Scripts:**
- `C:\Users\MIS INTERN\marrybrown_etl\api_etl\run_api_etl_oct2018.py`

**Backend:**
- `C:\Users\MIS INTERN\marrybrown_api\routers\sales_api_test.py`

**Portal:**
- `C:\Users\MIS INTERN\marrybrown-portal\src\pages\reports\DailySalesApiTestPage.jsx`

**Database:**
- Table: `dbo.fact_sales_transactions_api`
- Staging: `dbo.staging_sales_api`, `dbo.staging_sales_items_api`, `dbo.staging_payments_api`

**Raw Data:**
- `C:\Users\MIS INTERN\marrybrown_etl\api_data\raw_sales_oct2018_*.json`

---

## Success! What's Next?

### If API ETL Passes (≥99.97%)

**Option A: Deploy API ETL to Production**
1. Review schema enhancement SQL
2. Run on production database
3. Schedule API ETL daily (2 AM)
4. Monitor for 1 week
5. Retire Direct DB ETL

**Option B: Keep Hybrid Approach**
1. Use Direct DB for historical queries
2. Use API for daily incremental updates
3. Best of both worlds!

### If API ETL Fails (<99.97%)

**Keep Direct DB ETL:**
- Already proven at 99.999% accuracy
- Deploy to cloud as-is
- Get POC approved
- Revisit API later (post-FYP project)

---

**Good luck with testing!** 🚀

