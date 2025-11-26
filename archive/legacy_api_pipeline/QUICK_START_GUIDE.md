# Quick Start Guide - MarryBrown ETL

## 🚀 Running the Full ETL (Oct 2018 - Dec 2019)

### Step 1: Navigate to Project Directory
```bash
cd C:\laragon\www\marrybrown_etl
```

### Step 2: Run the Multi-Month ETL
```bash
python api_etl\run_cloud_etl_multi_month.py
```

That's it! The script will:
- ✅ Check which months are already loaded (smart resume)
- ✅ Clear staging tables
- ✅ Extract each month from Xilnex API
- ✅ Transform to fact table immediately after each month
- ✅ Skip already-loaded months automatically
- ✅ Handle crashes gracefully (resume from last completed month)

---

## 🧹 Clearing All Data (Fresh Start)

If you want to completely reset and start from scratch:

```bash
python api_etl\clear_etl_data.py
```

This will:
- Show current data counts
- Ask for confirmation (type "YES")
- Clear all staging and fact tables
- Verify deletion

---

## 📊 What to Expect

### Timeline
- **Per Month:** 15-30 minutes (depending on data volume)
- **Total (15 months):** 4-8 hours

### Progress Display
```
╔════════════════════════════════════════════════════════════════╗
║         CLOUD ETL - MULTI-MONTH EXTRACTION                     ║
║         October 2018 - December 2019 (15 months)               ║
╚════════════════════════════════════════════════════════════════╝

[CHECKING] Which months are already loaded...
  2018-10: ✓ Already loaded (1,234,567 rows)
  2018-11: ✗ Not loaded yet
  2018-12: ✗ Not loaded yet
  ...

[SUMMARY]
  Already Loaded: 1 months (SKIP)
  To Extract: 14 months
  
[INFO] Starting extraction in 5 seconds...
[INFO] Press Ctrl+C to cancel if needed...

[1/14] Processing November 2018...
  [Call 1] Fetching batch...
  Retrieved: 1000 sales
  Total fetched so far: 1000 sales
  ...
```

---

## ⚠️ Important Notes

### Smart Resume
- If the script crashes or is interrupted, just re-run it
- It will automatically skip already-loaded months
- Only processes months that are missing from fact_sales_transactions

### Staging Tables
- Cleared automatically at the start of each ETL run
- Used as temporary holding area for each month
- Data is transformed to facts immediately after each month

### API Limits
- Safety cap: 10,000 API calls per month (10M records capacity)
- Batch size: 1,000 records per call
- Persistent HTTP session for speed
- Adaptive pagination (fetches until API returns empty)

---

## 🔍 Monitoring Progress

### Check Current Data
```bash
python api_etl\clear_etl_data.py
# Then type "NO" when asked to confirm deletion
# This will show current counts without deleting anything
```

### View Loaded Months
The script shows this at the start:
- ✓ Already loaded months (with row counts)
- ✗ Months to be extracted

---

## 🐛 Troubleshooting

### Issue: Script stops with "MAX_API_CALLS reached"
**Solution:** This is a safety cap. If legitimate, increase `MAX_API_CALLS` in `api_etl/config_api.py`

### Issue: API returns 404 errors
**Solution:** Check that `mode=ByDateTime` parameter is in the URL (already fixed in extract_from_api.py line 143)

### Issue: Database connection timeout
**Solution:** Check `.env.cloud` credentials and ensure cloud server is accessible

### Issue: Want to re-run a specific month
**Solution:** 
1. Delete that month's data from fact_sales_transactions
2. Re-run the ETL - it will detect the missing month and process it

---

## 📁 Key Files

| File | Purpose |
|------|---------|
| `run_cloud_etl_multi_month.py` | Main orchestrator (15 months) |
| `extract_from_api.py` | API extraction logic |
| `transform_api_to_facts.py` | Staging → Fact transformation |
| `clear_etl_data.py` | Cleanup utility |
| `config_api.py` | API configuration |
| `.env.cloud` | Database credentials |

---

## 📊 Expected Results

After successful completion:

**fact_sales_transactions:**
- ~10-50 million rows (depending on actual sales volume)
- Date range: 2018-10-01 to 2019-12-31
- 15 distinct months

**Staging tables:**
- Empty (cleared at start, data transformed immediately)

**JSON files:**
- Saved in project root: `sales_October_2018.json`, etc.
- For reference/debugging only

---

## 🎯 Quick Commands Cheat Sheet

```bash
# Run full ETL
python api_etl\run_cloud_etl_multi_month.py

# Clear all data (fresh start)
python api_etl\clear_etl_data.py

# Check current data (without deleting)
python api_etl\clear_etl_data.py
# (then type "NO" when prompted)
```

---

**Last Updated:** November 7, 2025  
**Status:** Ready for Production ✅

