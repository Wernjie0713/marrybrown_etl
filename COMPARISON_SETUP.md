# API vs Warehouse Comparison - Setup Guide

## Quick Start

### Step 1: Update Configuration

Open `compare_api_vs_warehouse.py` and update these lines (around line 20):

```python
# Warehouse Connection - UPDATE THESE!
WAREHOUSE_SERVER = "localhost"  # or your SQL Server IP
WAREHOUSE_DATABASE = "MarryBrown_DW"
WAREHOUSE_USERNAME = "sa"
WAREHOUSE_PASSWORD = "your_password"  # ← Change this!
```

**If you're using Windows Authentication:**
```python
WAREHOUSE_USERNAME = None  # Set to None
WAREHOUSE_PASSWORD = None  # Set to None
```
And the script will use trusted connection.

### Step 2: Verify Test Parameters

The script is already configured for:
- **Date Range:** September 1-30, 2025
- **Outlet:** A FAMOSA

If you need different parameters, update these lines:
```python
TEST_OUTLET = "A FAMOSA"
TEST_START_DATE = "2025-09-01"
TEST_END_DATE = "2025-09-30"
```

### Step 3: Run the Script

```bash
python compare_api_vs_warehouse.py
```

### Step 4: Review Results

The script will:
1. ✅ Call Xilnex API for Sep 2025, A FAMOSA
2. ✅ Query your warehouse for same data
3. ✅ Compare side-by-side
4. ✅ Save API data to JSON file for reference

**Output will show:**
- Total Sales Count (API vs Warehouse)
- Total Items Count
- Total Payments Count
- Grand Total (RM)
- Net Amount (RM)
- Total Tax (RM)
- Payment Methods breakdown
- Sales Types breakdown

### Step 5: Manual Portal Comparison

After running the script:

1. Go to Xilnex Portal
2. Export Daily Sales Report for:
   - **Date:** September 1-30, 2025
   - **Outlet:** A FAMOSA
3. Save as: `xilnex_portal_export_sep2025_afamosa.xlsx`
4. Compare totals:
   - API Grand Total vs Portal Grand Total
   - Warehouse Grand Total vs Portal Grand Total
   - Which one matches better?

---

## Expected Scenarios

### Scenario 1: API Matches Portal Better
```
Portal:    RM 100,000.00
API:       RM 100,000.00  ✅ MATCH
Warehouse: RM  99,500.00  ❌ 0.5% off
```
**Decision:** Switch to API ETL (more accurate)

### Scenario 2: Warehouse Matches Portal Better
```
Portal:    RM 100,000.00
API:       RM  99,500.00  ❌ 0.5% off
Warehouse: RM 100,000.00  ✅ MATCH
```
**Decision:** Keep direct DB ETL (already accurate)

### Scenario 3: Both Match Perfectly
```
Portal:    RM 100,000.00
API:       RM 100,000.00  ✅ MATCH
Warehouse: RM 100,000.00  ✅ MATCH
```
**Decision:** Choose API (easier to maintain, no DB investigation needed)

### Scenario 4: Neither Matches Portal
```
Portal:    RM 100,000.00
API:       RM  99,500.00  ❌ 0.5% off
Warehouse: RM  98,000.00  ❌ 2.0% off
```
**Decision:** Investigate discrepancies, API is closer

---

## Troubleshooting

### Connection Error
```
ERROR querying warehouse: Login failed
```
**Fix:** Update `WAREHOUSE_PASSWORD` or use Windows Authentication

### API Returns No Data
```
Total sales retrieved: 0
```
**Fix:** Check if:
- API token is still enabled in Xilnex
- Date range has data (September 2025)
- Outlet name spelling is correct ("A FAMOSA" vs "A FAMOSA ")

### Script Takes Long Time
```
[Call 1] Fetching batch...
[Call 2] Fetching batch...
...
```
**This is normal!** API returns 1000 records per call. September 2025 might need multiple calls.
Typical time: 1-5 minutes for one month of data.

---

## What Happens Next?

Based on results:

**If API wins:**
- Build full API-based ETL
- Migrate from direct DB queries
- Simpler maintenance going forward

**If Warehouse wins:**
- Keep current ETL
- Deploy to cloud as-is
- Focus on validation completion

**Either way:** You have a working, validated system ready for POC!

