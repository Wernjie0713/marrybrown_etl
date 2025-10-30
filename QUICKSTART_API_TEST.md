# 🚀 Quick Start - API ETL Testing

**Time Required:** 15-20 minutes  
**Goal:** Export Excel from API ETL and compare with Xilnex portal

---

## Step 1: Database Setup (5 minutes)

```bash
cd C:\Users\MIS INTERN\marrybrown_etl

# Run both schema scripts
sqlcmd -S localhost -d MarryBrown_DW -E -i create_fact_table_api.sql
sqlcmd -S localhost -d MarryBrown_DW -E -i schema_enhancements_for_api.sql
```

**Note:** Adjust `-S localhost` to your SQL Server name if different. Use `-E` for Windows Auth, or `-U username -P password` for SQL Auth.

---

## Step 2: Run API ETL (10 minutes)

```bash
cd C:\Users\MIS INTERN\marrybrown_etl

# Activate virtual environment
venv\Scripts\activate

# Run ETL (extracts October 2018 from API)
python api_etl\run_api_etl_oct2018.py
```

**What to expect:**
- 5-10 API calls to retrieve October 2018 data
- ~2,400 sales extracted
- ~9,000 fact rows created
- Raw JSON saved to `api_data/` folder

---

## Step 3: Start Backend (1 minute)

**New terminal window:**
```bash
cd C:\Users\MIS INTERN\marrybrown_api
venv\Scripts\activate
uvicorn main:app --reload
```

Leave this running!

---

## Step 4: Start Portal (1 minute)

**New terminal window:**
```bash
cd C:\Users\MIS INTERN\marrybrown-portal
npm run dev
```

Leave this running!

---

## Step 5: Export from Portal (2 minutes)

1. Open: http://localhost:5173
2. Login
3. Click "Reports"
4. Click "🧪 Daily Sales (API Test)"
5. Set dates: **Oct 1, 2018** to **Oct 31, 2018**
6. Click "Run Report"
7. Wait for table to load (~2,400 records)
8. Click "Export to Excel"
9. Save as: `My_Portal_Export_Oct2018.xlsx`

---

## Step 6: Export from Xilnex Portal (3 minutes)

1. Login to Xilnex portal
2. Go to Reports → Daily Sales
3. Set dates: **Oct 1-31, 2018**
4. Generate & Export
5. Save as: `Xilnex_Export_Oct2018.xlsx`

---

## Step 7: Compare (2 minutes)

Open both Excel files side-by-side.

**Compare these totals:**

| Metric | My Portal | Xilnex | Match? |
|--------|----------|--------|--------|
| Total Sales Amount | RM _____ | RM _____ | ⬜ |
| Total Profit Amount | RM _____ | RM _____ | ⬜ |
| Record Count | _____ | _____ | ⬜ |

**Calculate accuracy:**
```
Accuracy = (My Portal Amount / Xilnex Amount) × 100

Target: ≥99.97%
```

---

## ✅ Success!

**If ≥99.97% match:**
- API ETL is accurate enough!
- Consider using for future daily updates
- Document results

**If <99.97% match:**
- Investigate discrepancies
- Or stick with proven Direct DB ETL (99.999%)
- Either way, your POC is ready!

---

## 🆘 Quick Troubleshooting

**API returns 0 sales:**
```bash
# Check API token is enabled in Xilnex
python test_xilnex_sync_api.py
```

**Portal shows no data:**
```bash
# Check fact table has data
sqlcmd -S localhost -d MarryBrown_DW -Q "SELECT COUNT(*) FROM fact_sales_transactions_api"
```

**Backend error:**
```bash
# Check logs in terminal where uvicorn is running
# Common issue: database connection string in .env
```

---

**That's it! Quick and simple.** 🎉

For detailed guide, see `TESTING_API_ETL.md`

