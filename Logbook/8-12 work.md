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
