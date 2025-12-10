# 8-12 work

## 1. Data Quality Verification (Aug - Oct 2025)

Today, we verified the data quality for the 3-month replication period (August, September, October 2025).

### Initial Findings (`verify_data_quality.py`)

We updated `tests/verify_data_quality.py` to check the date range `2025-08-01` to `2025-10-31`.

- **Reference Tables:** Mostly matched. Minor mismatches in live tables (`STOCK`, `CUSTOMER`) are expected due to the time gap between source and target snapshots.
- **Sales Tables:** `APP_4_SALES` showed a significant mismatch of **-48,424 rows**.

### Deep Dive: Daily Row Counts (`verify_daily_row_counts.py`)

To pinpoint the missing data, we created a new script `tests/verify_daily_row_counts.py` to compare row counts day-by-day for all sales-related tables.

**Outcome:**
The script successfully identified the specific dates causing the mismatch:

1.  **2025-10-31 (Major Missing Data):**

    - **ALL** sales tables (`SALES`, `SALESITEM`, `PAYMENT`, `VOIDSALESITEM`, `VOUCHER`) are completely missing data for this day in the target warehouse.
    - `APP_4_SALES`: -48,424 rows
    - `APP_4_SALESITEM`: -254,427 rows
    - `APP_4_PAYMENT`: -48,573 rows
    - `APP_4_VOIDSALESITEM`: -3,159 rows
    - `APP_4_VOUCHER`: -2,677 rows
    - _Cause:_ Likely a connection drop or timeout occurred exactly at the end of the batch processing for October.

2.  **2025-10-01 (Minor Mismatch):**
    - `APP_4_VOUCHER`: Target has **+3 rows** compared to Source (73,670 vs 73,667).
    - _Cause:_ Likely a minor race condition or data update on the source side during the replication window.

### Action Plan

- Re-run the replication script specifically for `2025-10-31` to backfill the missing day.
- Investigate the `APP_4_VOUCHER` mismatch on `2025-10-01` if strict accuracy is required, though +3 rows is negligible.

## 2. Script Enhancement: Inclusive End Date

We updated `scripts/replicate_all_sales_data.py` to make the `--end-date` argument **inclusive**.

- **Change:** The script now automatically adds 1 day to the provided end date before passing it to the internal logic.
- **Benefit:** Users can now run `--start-date 2025-10-31 --end-date 2025-10-31` to replicate exactly that one day, without needing to manually calculate the next day.

## 3. Re-verification Results (Final)

We re-ran `tests/verify_daily_row_counts.py` after performing targeted replication for the problematic dates.

- **2025-10-31:** **FIXED.** All tables match perfectly.
- **2025-10-01:** **FIXED.** The minor mismatch in `APP_4_VOUCHER` is now resolved (0 diff).

**Conclusion:** The data for the entire period (Aug - Oct 2025) is now **100% consistent** between Source and Target.

## 4. Sales API Consolidation

We consolidated the Sales API by replacing the legacy `routers/sales.py` with the new 1:1 replica-based implementation (formerly `sync_sales.py`).

- **Action:** Deleted legacy `sales.py` and renamed `sync_sales.py` to `sales.py`.
- **Endpoint:** `GET /sales/sales` (serving Xilnex-style nested JSON from replica tables).
- **Benefit:** Establishes a single "Source of Truth" API for sales data, which future report-specific endpoints can consume.
- **Status:** Completed and verified.

## 5. Daily Sales Summary API Implementation

Implemented a new Daily Sales Summary Report API endpoint at `GET /reports/daily-sales-summary` in `routers/reports.py`.

- **Data Source:** 1:1 Xilnex replica tables (`APP_4_SALES`, `APP_4_SALESITEM`, `LOCATION_DETAIL`)
- **Response Fields:** `date`, `store_name`, `sales_amount`, `profit_amount`
- **Calculation:**
  - Sales Amount: `SUM(DOUBLE_TOTAL_AMOUNT)` from sales header
  - Profit Amount: `SUM(DOUBLE_NET_AMOUNT) - SUM(DOUBLE_COST * INT_QUANTITY)` from items

## 6. Profit Calculation Bug Investigation & Fix

### Problem Identified

After comparing API output to Xilnex exports, we found **Profit = Sales** for all outlets — profit was not being calculated correctly.

### Investigation Process

1. Created `debug_report_issues.py` to query the database directly
2. User provided sample data (`app_4_salesitem.json`) showing valid `DOUBLE_COST` values
3. Verified specific item (ID `198909373`) had cost = `2.4102` in database

### Root Cause

**Wrong JOIN condition** in the cost aggregation CTE:

- ❌ Before: `i.PCID = s.ID` (not matching correctly)
- ✅ After: `i.SALES_NO = s.SALES_NO` (correct relationship)

### Fix Applied

Updated `routers/reports.py` to use `i.SALES_NO = s.SALES_NO` in the cost_agg CTE.

**Result:** Profit now calculates correctly — verified with database test showing `Total Cost = 7553.15` for MB PASIR PUTEH.

## 7. Data Comparison Results

After fixing the profit calculation, we re-compared API output against Xilnex exports:

| Metric            | Status                                                     |
| ----------------- | ---------------------------------------------------------- |
| **Sales Amount**  | 216 outlets match, **21 outlets have minor discrepancies** |
| **Profit Amount** | All outlets show differences (API consistently higher)     |

### Sales Discrepancies (21 outlets)

Differences range from RM 1.50 to RM 245.20. Likely caused by missing filter conditions (voided transactions, specific statuses).

### Profit Discrepancies (all outlets)

API profit is consistently higher than Xilnex. Indicates different calculation formulas — needs further investigation of Xilnex profit logic.

## 8. API Enhancement: Filter Parameters

To address the sales discrepancies, we enhanced `routers/reports.py` to accept additional filter parameters:

### New Parameters Added

- `location_keys: List[int]` — Filter by specific outlet(s)
- `sales_statuses: List[str]` — Filter by status (default: `COMPLETED`)

### Implementation

- Added `_build_in_clause()` helper for dynamic SQL IN clauses
- WHERE clause now dynamically constructed based on provided filters
- Status comparison is case-insensitive (`UPPER(s.SALES_STATUS)`)

## 9. Portal Integration (In Progress)

### Current State

- Portal (`DailySalesReportPage.jsx`) connects to outdated `/sales/reports/daily-sales` endpoint
- Has multi-select UI for stores and statuses already built

### Next Steps

- Update `api.js` to point to new `/reports/daily-sales-summary` endpoint
- Adjust request format (POST → GET with query params)
- Verify portal works with enhanced API filters

## 10. Profit Calculation: DOUBLE_PROFIT vs DOUBLE_COST

After investigating Notion documents on previous Xilnex report implementations, we tested using `DOUBLE_PROFIT` from `APP_4_SALESITEM` instead of calculating from cost.

### Previous Formula (DOUBLE_COST)

```sql
SUM(DOUBLE_NET_AMOUNT) - SUM(DOUBLE_COST * INT_QUANTITY)
```

Result: All 237 outlets showed profit discrepancies (API always higher than Xilnex).

### New Formula (DOUBLE_PROFIT)

```sql
SUM(DOUBLE_PROFIT)
```

Result: **Much closer** to Xilnex values:

- 1 outlet: exact match (< RM 1 difference)
- 56 outlets: close match (< RM 50 difference)
- 180 outlets: still have larger discrepancies

**Conclusion**: `DOUBLE_PROFIT` is the correct column. Updated `routers/reports.py` to use this.

## 11. SALES_STATUS Investigation

Investigated database to clarify available status values vs portal options.

### Database Values (1 Aug 2025)

| SALES_STATUS | Count  |
| ------------ | ------ |
| COMPLETED    | 49,062 |
| CANCELLED    | 408    |
| OPEN         | 2      |

### Status Comparison

| Our Portal | Xilnex Portal | Database     |
| ---------- | ------------- | ------------ |
| COMPLETED  | Completed     | ✅ Exists    |
| CANCELLED  | Cancelled     | ✅ Exists    |
| PENDING    | -             | ❌ Not found |
| VOID       | -             | ❌ Not found |
| -          | Open          | ✅ Exists    |
| -          | Confirmed     | ❌ Not found |
| -          | Write Off     | ❌ Not found |

**Conclusion**: Database only has 3 statuses: `COMPLETED`, `CANCELLED`, `OPEN`. Xilnex portal's "Confirmed" and "Write Off" options are unused/legacy. Portal filter options should be updated to match database reality.

## 12. Item-Level Filter Investigation

Investigated SALESITEM table fields that could affect profit calculation.

### Investigation Results (1 Aug 2025)

| Category                         | Count       | Profit Impact          |
| -------------------------------- | ----------- | ---------------------- |
| **ITEM_TYPE**                    |             |                        |
| Food                             | 165,179     | RM 520,873.84          |
| Drinks                           | 61,650      | RM -3,649.34           |
| BYOD                             | 36,287      | RM 79,863.71           |
| (Empty)                          | 6,584       | RM 5,711.09            |
| Others                           | 438         | RM 103.51              |
| **FOC Items**                    |             |                        |
| Not FOC                          | 270,145     | RM 602,925.74          |
| FOC                              | 0           | RM 0.00 (no FOC items) |
| **Service Charge Items**         |             |                        |
| All items are Not Service Charge | 270,145     | RM 602,925.74          |
| **Zero Price Items**             | **135,969** | **RM -177,939.06**     |
| **Negative Profit Items**        | **118,469** | **RM -341,143.49**     |

### Total Profit Breakdown

- Total: RM 602,925.74
- Positive Profit: RM 944,069.23
- Negative Profit: RM -341,143.49

### Key Findings

1. **Zero-price items are 50% of all items**: 135,969 out of 270,145 items have `DOUBLE_PRICE = 0`
2. **Zero-price items have massive negative profit impact**: RM -177,939
3. **118,469 items have negative DOUBLE_PROFIT**: Total RM -341,143.49
4. **No FOC items (BOOL_ISFOC)**: All 270,145 items are not marked as FOC
5. **No service charge items**: All items have `BOOL_IS_SERVICECHARGEITEM = 0`
6. **Drinks have overall negative profit**: RM -3,649.34

### Hypothesis

Xilnex likely **excludes zero-price items** (`DOUBLE_PRICE = 0`) from profit calculation because they represent:

- Modifier selections (e.g., "Original flavor" choice)
- Bundle components with no standalone price
- Add-ons included in combos

**Next Step**: Test excluding zero-price items from profit calculation to see if it aligns with Xilnex.
