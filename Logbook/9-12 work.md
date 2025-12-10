# 9-12 work

## 1. Zero-Price Item Exclusion Experiment

### Background

Based on yesterday's item-level filter investigation, we hypothesized that Xilnex excludes zero-price items (`DOUBLE_PRICE = 0`) from profit calculation because:

- 135,969 items (50% of all items) have zero price
- These items have RM -177,939 negative profit impact
- They represent modifiers, bundle components, and combo add-ons

### Experiment

Updated `routers/reports.py` to add filter:

```sql
AND CAST(i.DOUBLE_PRICE AS float) > 0
```

### Results

| Metric                | Before (DOUBLE_PROFIT) | After (Exclude Zero-Price) |
| --------------------- | ---------------------- | -------------------------- |
| Exact match (< RM 1)  | 1                      | 0                          |
| Close match (< RM 50) | 56                     | 2                          |
| Far off (>= RM 50)    | 180                    | **234**                    |
| Avg profit diff       | RM 120.78              | **RM 900.55**              |

### Conclusion

**Hypothesis was WRONG.** Excluding zero-price items made the discrepancy **significantly worse**.

Xilnex **includes** zero-price items in profit calculation. The negative profit from modifiers/bundle components is intentionally part of their calculation.

### Action

Reverted the zero-price exclusion filter. API now uses `SUM(DOUBLE_PROFIT)` without item-level filtering.

## 2. Header-Level DOUBLE_PROFIT Discovery

### Investigation

Searched the Xilnex schema (`xilnex_full_schema.json`) and discovered that `APP_4_SALES` has its own `DOUBLE_PROFIT` column at the transaction header level, separate from item-level profits.

### Comparison (1 Aug 2025)

| Source                           | Profit        |
| -------------------------------- | ------------- |
| Header-level (s.DOUBLE_PROFIT)   | RM 574,165.71 |
| Item-level (SUM i.DOUBLE_PROFIT) | RM 602,925.74 |
| Difference                       | RM 28,760.03  |

### API Update

Changed `routers/reports.py` to use header-level `SUM(s.DOUBLE_PROFIT)` from `APP_4_SALES` instead of joining to `APP_4_SALESITEM`.

### Results

| Metric                | Item-Level (Before) | Header-Level (After) |
| --------------------- | ------------------- | -------------------- |
| Exact match (< RM 1)  | 1                   | **214** ✅           |
| Close match (< RM 50) | 56                  | **230**              |
| Far off (>= RM 50)    | 180                 | **6**                |
| Avg profit diff       | RM 120.78           | **RM -1.08** ✅      |

### Precision Check

| Threshold        | Stores Match |
| ---------------- | ------------ |
| diff < RM 1      | 214          |
| diff < RM 0.1    | 212          |
| diff = 0 (exact) | 203          |

### Remaining Discrepancies (6 stores with diff >= RM 50)

| Store                     | Our Profit | Xilnex  | Diff    |
| ------------------------- | ---------- | ------- | ------- |
| MB SERI MANJUNG           | 3903.57    | 3794.25 | +109.33 |
| MB PRESINT 15 PUTRAJAYA   | 2838.14    | 2729.75 | +108.39 |
| MB RIMBUNAN KEPONG        | 2560.11    | 2493.40 | +66.71  |
| MB PRIMA SQUARE SANDAKAN  | 5130.41    | 5066.50 | +63.91  |
| MB PETRON JALAN BADLISHAH | 2234.68    | 2179.44 | +55.24  |
| MB BANDAR BARU SELAYANG   | 3578.95    | 3534.25 | +44.70  |

### Conclusion

**Header-level `DOUBLE_PROFIT` is the correct approach.** The remaining minor differences (6 stores) are likely due to timing/data sync issues between our replica and Xilnex's live data.

## 3. Data Quality Verification (Jan-Oct 2025)

### Background

ETL replication was extended from Aug-Oct 2025 to cover Jan-Oct 2025. However, the process was interrupted due to system disk running full. Data was moved to E: disk (data disk).

### Verification Results

Ran `tests/check_monthly_counts.py` to compare `APP_4_SALES` row counts:

| Month     | Source     | Target     | Diff    | Status     |
| --------- | ---------- | ---------- | ------- | ---------- |
| Jan 2025  | 1,520,467  | 1,520,467  | 0       | ✅ OK      |
| Feb 2025  | 1,279,824  | 1,279,824  | 0       | ✅ OK      |
| Mar 2025  | 1,281,357  | 1,281,357  | 0       | ✅ OK      |
| Apr 2025  | 1,531,607  | 1,531,607  | 0       | ✅ OK      |
| May 2025  | 1,514,039  | 1,514,039  | 0       | ✅ OK      |
| Jun 2025  | 1,465,735  | 1,465,735  | 0       | ✅ OK      |
| Jul 2025  | 1,408,002  | 1,363,132  | -44,870 | ❌ MISSING |
| Aug 2025  | 1,439,072  | 1,439,072  | 0       | ✅ OK      |
| Sep 2025  | 1,382,392  | 1,382,392  | 0       | ✅ OK      |
| Oct 2025  | 1,520,173  | 1,520,173  | 0       | ✅ OK      |
| **TOTAL** | 14,342,668 | 14,297,798 | -44,870 |            |

### Findings

- **Only July 2025 has missing data** (44,870 rows = ~3.2% of July)
- Disk full error occurred during July 2025 replication
- All other months (Jan-Jun, Aug-Oct) are complete ✅

### Next Steps

Re-run replication for July 2025 only:

```powershell
python scripts/replicate_all_sales_data.py --start-date 2025-07-01 --end-date 2025-07-31
```

## 4. Cross-Date Validation (25-Oct-2025)

### Comparison Results

| Metric                | 1-Aug-2025 | 25-Oct-2025 |
| --------------------- | ---------- | ----------- |
| Total stores          | 237        | 242         |
| Exact match (< RM 1)  | 214        | **230** ✅  |
| Close match (< RM 50) | 230        | **240**     |
| Far off (>= RM 50)    | 6          | **2**       |
| Avg profit diff       | RM -1.08   | RM 1.67     |

### Stores with Discrepancy >= RM 50 (25-Oct-2025)

| Store                    | MB Profit | Xilnex  | Diff    |
| ------------------------ | --------- | ------- | ------- |
| MB KUALA KRAI            | 6605.82   | 6444.83 | +160.99 |
| MB PETRON MAKMUR GAMBANG | 3093.24   | 3043.03 | +50.21  |

### 1-Aug Problematic Stores on 25-Oct

| Store                     | Status                |
| ------------------------- | --------------------- |
| MB SERI MANJUNG           | ✅ OK NOW (Diff=0.00) |
| MB PRESINT 15 PUTRAJAYA   | ✅ OK NOW (Diff=0.00) |
| MB RIMBUNAN KEPONG        | ✅ OK NOW (Diff=0.00) |
| MB PRIMA SQUARE SANDAKAN  | ✅ OK NOW (Diff=0.00) |
| MB PETRON JALAN BADLISHAH | ✅ OK NOW (Diff=0.00) |
| MB BANDAR BARU SELAYANG   | ✅ OK NOW (Diff=0.00) |

### Conclusion

- **Different stores** have issues on different dates - not a systemic problem with specific outlets
- The 6 stores with issues on 1-Aug are **all OK on 25-Oct**
- Discrepancies are likely timing/sync issues between our data and Xilnex portal
- **Overall accuracy is excellent**: 230/242 stores exact match (95%)

## 5. Stable Data Validation (1-Jan-2025)

### Background

Used 1-Jan-2025 data which was synced yesterday and should be stable (no more modifications from Xilnex).

### Results Summary

| Metric                | Sales Amount | Profit Amount |
| --------------------- | ------------ | ------------- |
| Exact match (< RM 1)  | 204          | 204           |
| Very close (< RM 10)  | 206          | 209           |
| Close match (< RM 50) | 219          | 222           |
| Far off (>= RM 50)    | 7            | 4             |
| Avg diff              | RM 14.19     | RM 6.52       |

### Perfect Match: 204/226 stores = 90.3%

### Top Sales Discrepancies

| Store              | MB     | Xilnex | Diff       |
| ------------------ | ------ | ------ | ---------- |
| MB MYDIN TUNJONG   | 15,153 | 13,027 | **+2,125** |
| MB TAMAN MAJU JAYA | 11,166 | 10,977 | +188       |
| MB KLUANG MALL     | 5,779  | 5,601  | +178       |

### Top Profit Discrepancies

| Store              | MB    | Xilnex | Diff     |
| ------------------ | ----- | ------ | -------- |
| MB MYDIN TUNJONG   | 6,752 | 5,886  | **+866** |
| MB TAMAN MAJU JAYA | 5,737 | 5,632  | +105     |
| MB KLUANG MALL     | 3,266 | 3,167  | +98      |

### Key Findings

1. **90.3% perfect match** for both sales and profit
2. All differences are **positive** (our API > Xilnex) - suggests extra transactions in DB
3. Same stores appear in both sales and profit discrepancies
4. **MB MYDIN TUNJONG** has largest gap: +RM 2,125 sales

## 6. Root Cause: Return Transactions Fix

### Investigation

Drilled into MB MYDIN TUNJONG (1-Jan-2025) to understand RM 2,125 discrepancy:

```
=== Sales by Type (COMPLETED only) ===
  Delivery: 36 txns, Sales=RM 1,341.44
  Dine In: 92 txns, Sales=RM 4,600.40
  Return: 1 txn, Sales=RM -2,125.70  ← NEGATIVE amount!
  Take Away: 141 txns, Sales=RM 9,211.75
```

### The Problem

Our API had filter: `SALES_TYPE <> 'Return'`

This was **WRONG** because:

- Return transactions have **negative amounts** (e.g., -2,125.70)
- Xilnex **INCLUDES** returns - the negative values reduce the total
- Our API **EXCLUDED** returns - so we were missing the deduction

### The Fix

Removed `(s.SALES_TYPE IS NULL OR s.SALES_TYPE <> 'Return')` from `routers/reports.py`

### Verification Results

| Date        | Before Fix | After Fix     |
| ----------- | ---------- | ------------- |
| 1-Jan-2025  | 90.3%      | **100.0%** ✅ |
| 1-Aug-2025  | ~90%       | **100.0%** ✅ |
| 25-Oct-2025 | ~95%       | **100.0%** ✅ |

> **Note:** Initial 1-Aug comparison showed 99.6% (236/237) because Xilnex Excel export includes a "Grand Total" summary row. This is not a real store. All 236 actual stores match 100%.

### Conclusion

**Daily Sales Summary API now achieves 100% accuracy** across all tested dates for all actual store locations.

## 7. CANCELLED Sales Verification

Verified with CANCELLED sales status for Oct 2025:

| Metric        | Value         |
| ------------- | ------------- |
| MB stores     | 130           |
| Xilnex stores | 130           |
| Perfect match | **100.0%** ✅ |

## Final Summary

| Sales Status | Test Dates                | Match Rate  |
| ------------ | ------------------------- | ----------- |
| COMPLETED    | 1-Jan, 1-Aug, 25-Oct 2025 | **100%** ✅ |
| CANCELLED    | Oct 2025                  | **100%** ✅ |

### Key Fixes Applied

1. **Header-level DOUBLE_PROFIT** - Use pre-calculated profit from `APP_4_SALES.DOUBLE_PROFIT` instead of item-level sum
2. **Include Return transactions** - Removed `SALES_TYPE <> 'Return'` filter; returns have negative amounts that naturally reduce totals

### API Endpoint

```
GET /reports/daily-sales-summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&sales_statuses=COMPLETED
```

**Daily Sales Summary API is now production-ready with 100% accuracy.**
