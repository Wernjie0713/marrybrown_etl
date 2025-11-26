# Changelog - Marrybrown ETL Pipeline

All notable changes to the ETL pipeline will be documented in this file.

---

## [1.8.0] - 2025-10-21

### 📊 PHASE 1A COMPLETE - Complete Schema Catalog

**Major Achievement**: Completed comprehensive schema documentation for all 10 core Xilnex sales tables with 100% completeness.

**Final Deliverable**:
- `Xilnex_Schema_Catalog_FULLY_COMPLETE_20251021_114614.xlsx`
- **969 columns** documented across 10 tables
- **64 columns** actively used in ETL (YES)
- **905 columns** documented as not used (NO)
- **ZERO blank cells** - 100% completeness

**What Was Accomplished**:
1. ✅ **Complete Schema Discovery**
   - All 10 core sales tables documented
   - All 969 columns cataloged with metadata (Data Type, Max Length, Nullable)
   
2. ✅ **ETL Mapping**
   - All 64 actively-used columns verified and mapped
   - Warehouse Column and Warehouse Table documented
   - Automated mapping from Notion ETL docs and `transform_sales_facts.sql`
   
3. ✅ **Priority & Notes**
   - 775 blank cells filled with context-aware information
   - Priority assigned (HIGH/MEDIUM/LOW) based on column purpose
   - Context-specific notes for every column (security, system, custom fields, etc.)
   - 420 HIGH/MEDIUM priority columns identified for Phase 2
   
4. ✅ **Standardization**
   - Standardized "Used?" values to YES/NO (all caps)
   - Color-coded formatting (Green=YES, Red=NO)
   - Consistent structure across all 10 tables

**Tables Documented**:
1. APP_4_CUSTOMER (160 columns) - Customer master data
2. APP_4_PAYMENT (70 columns) - Payment methods
3. APP_4_SALES (184 columns) - Sales headers (invoices)
4. APP_4_VOIDSALESITEM (36 columns) - Void tracking (Phase 2)
5. APP_4_ITEM (190 columns) - Product master data
6. APP_4_SALESITEM (197 columns) - Line items (CRITICAL)
7. APP_4_VOUCHER_MASTER (64 columns) - Promotions/vouchers
8. APP_4_CASHIER_DRAWER (29 columns) - Terminal master data
9. APP_4_REGISTERLOG (25 columns) - Cash reconciliation (Phase 2)
10. LOCATION_DETAIL (14 columns) - Store/location master

**Automation Tools Created** (cleaned up after use):
- Schema discovery scripts
- ETL mapping automation
- Blank cell completion with context-aware notes
- Standardization and validation tools

### 🧹 Codebase Cleanup

**Files Removed** (21 temporary files):
- 13 Python scripts (catalog generation, updating, verification)
- 7 JSON files (intermediate column data)
- 1 SQL file (schema query)

**Files Retained**:
- ✅ Final production catalog Excel file
- ✅ Core ETL pipeline scripts
- ✅ Documentation files (README, CHANGELOG, Phase 1A summaries)

### 📝 Documentation Updates

**Notion Document Hub**:
- ✅ Updated "Phase 1A: Complete Schema Catalog - Findings" to 100% complete
- ✅ Documented 775 blank cell completions
- ✅ Documented 420 HIGH/MEDIUM priority missing columns
- ✅ Final statistics and achievement summary

**Local Documentation**:
- ✅ `PHASE_1A_COMPLETION_SUMMARY.md` - Initial completion
- ✅ `PHASE_1A_FINAL_COMPLETION_SUMMARY.md` - Final details
- ✅ `PHASE_1A_CLEANUP_SUMMARY.md` - Cleanup summary

### 🎯 Business Value

**Schema Completeness**:
- 100% visibility into source system structure
- Foundation for all future ETL enhancements
- Reference documentation for team scalability
- Automated discovery approach (reusable for other schemas)

**Data Quality**:
- Verified all actively-used columns are captured
- Identified 420 potential enhancement opportunities
- Context-aware documentation for maintenance

**Professional Standards**:
- Production-ready catalog with zero blanks
- Clean, maintainable codebase
- Comprehensive documentation
- Ready for Phase 1B validation

### ✅ Phase 1A Status: COMPLETE

**Ready for Phase 1B**: Report-Driven Validation
- Multi-outlet data quality testing
- Tolerance threshold setting (<1% variance)
- Comprehensive report validation

---

## [1.7.0] - 2025-10-16

### 🎯 NEW FEATURE - Split-Tender Payment Allocation

**Major Enhancement**: The ETL now properly handles invoices with multiple payment methods (split-tender transactions) by allocating amounts proportionally across all payment methods used.

**Problem Before**:
- Only ONE payment method was stored per invoice (the one with priority based on `card_type`)
- Split-tender transactions (e.g., RM 20 voucher + RM 45.90 card) had their **entire amount** attributed to a single payment method
- Payment breakdowns in reports were **inaccurate** for split-tender invoices
- Example: Invoice 1527047884 (RM 20 voucher + RM 45.90 card) was reported as **RM 65.90 Card only**

**Solution Now**:
- **Proportional Allocation**: Each payment method gets its fair share of each line item
- **Multiple Fact Rows**: One fact row per line item per payment method
- **Accurate Breakdowns**: Reports now show correct payment method distributions

**How It Works**:
1. Calculate total payment amount per invoice (e.g., RM 65.90 total)
2. Calculate allocation percentage per payment method:
   - Voucher: RM 20 / RM 65.90 = 30.35%
   - Card: RM 45.90 / RM 65.90 = 69.65%
3. Create multiple fact rows per line item (one per payment method)
4. Allocate all amounts proportionally:
   - Line item worth RM 64.90 → Voucher: RM 19.70, Card: RM 45.20
   - Line item worth RM 1.00 → Voucher: RM 0.30, Card: RM 0.70

**Example - Invoice 1527047884**:
```
Before (Broken):
- 6 line items × 1 payment = 6 fact rows
- Card: RM 65.90 ❌
- Voucher: RM 0 ❌

After (Fixed):
- 6 line items × 2 payments = 12 fact rows
- Card: RM 45.90 ✅ (69.65% allocation)
- Voucher: RM 20.00 ✅ (30.35% allocation)
```

### 📊 Schema Changes
- `dbo.staging_payments`: Added `double_amount DECIMAL(18,4)` column to store payment amounts

### 🔧 Files Modified
- `etl_fact_sales_historical.py`: Added `double_amount` to payment extraction query
- `transform_sales_facts.sql`: Complete rewrite with proportional allocation logic using CTEs:
  - `InvoicePaymentTotals`: Calculates total payments per invoice
  - `PaymentAllocations`: Calculates allocation percentage per payment method
  - `LineItemsWithPayments`: Cross-joins line items with payment allocations

### 💡 Technical Implementation
```sql
-- New CTE structure:
WITH InvoicePaymentTotals AS (
    -- Sum all payments per invoice
    SELECT invoice_id, SUM(double_amount) as total_payment_amount
    FROM staging_payments
    GROUP BY invoice_id
),
PaymentAllocations AS (
    -- Calculate percentage for each payment method
    SELECT 
        invoice_id, method, card_type,
        double_amount / total_payment_amount as allocation_percentage
    FROM staging_payments
    JOIN InvoicePaymentTotals USING (invoice_id)
),
LineItemsWithPayments AS (
    -- Allocate line item amounts proportionally
    SELECT 
        *,
        double_sub_total * allocation_percentage AS allocated_total_amount
    FROM staging_sales_items
    JOIN staging_sales USING (sales_no)
    LEFT JOIN PaymentAllocations ON invoice_id = sales_no
)
```

### 📈 Impact on Reports
- **EOD Sales Summary Report**: Now shows accurate payment method breakdown
- **Daily Sales Report**: Payment totals now match sales totals exactly
- **Data Quality**: Portal is now MORE accurate than Xilnex (which has rounding issues in split-tender scenarios)

### ⚠️ Important Notes
- **Fact Row Count Increase**: Invoices with N payment methods now generate N times more fact rows
- **Performance**: Query performance remains excellent due to proper indexing
- **Backwards Compatibility**: Old queries still work; new queries get accurate split-tender data

### 🔧 Migration Steps
1. Add `double_amount` column to `staging_payments`:
   ```sql
   ALTER TABLE dbo.staging_payments ADD double_amount DECIMAL(18,4);
   ```
2. Re-run ETL for affected date ranges to populate new payment allocations
3. Reports automatically reflect accurate split-tender data

### 📝 Documentation Tasks
- [x] Update ETL CHANGELOG with split-tender allocation feature
- [x] Update API documentation with payment allocation notes
- [x] Update Notion: ETL technical analysis (ETL Source-to-Target Mapping, Staging Tables Schema, transform_sales_facts.sql)
- [x] Update Notion: Data quality improvements (Created "EOD Sales Summary Report - Technical Analysis & Findings" document)

---

## [1.6.0] - 2025-10-15

### 🔧 Fixed - Tax Calculation Accuracy

**Issue**: "Sales Amount ex. Tax" (NetAmount) was understated compared to Xilnex portal due to incomplete tax subtraction.

**Root Cause**: Transform SQL was using `double_mgst_tax_amount` (item-level tax only) instead of `DOUBLE_TOTAL_TAX_AMOUNT` (item + bill-level tax allocation).

**Impact**: 
- Revenue figures were **overstated** by the amount of bill-level tax allocation
- Example: RM 10.95 overstatement on a single catering transaction (RM 206.40 total)
- Affected all reports showing "Sales ex. Tax" or pre-tax revenue

**Fix**:
- Added `DOUBLE_TOTAL_TAX_AMOUNT` extraction from `APP_4_SALESITEM`
- Added `double_total_tax_amount` column to `staging_sales_items` table
- Updated `transform_sales_facts.sql` to use `DOUBLE_TOTAL_TAX_AMOUNT` for NetAmount and GrossAmount calculations
- Updated TaxAmount field to use `DOUBLE_TOTAL_TAX_AMOUNT` instead of `double_mgst_tax_amount`

### 📊 Schema Changes
- `dbo.staging_sales_items`: Added `double_total_tax_amount DECIMAL(18,4)`

### 🔧 Migration Scripts
- `alter_staging_sales_items_add_total_tax.sql`

### 💡 Technical Details

**Tax Field Breakdown**:
- `double_mgst_tax_amount`: Item-level MGST tax only
- `DOUBLE_TOTAL_BILL_LEVEL_TAX_AMOUNT`: Bill-level tax allocated to item
- `DOUBLE_TOTAL_TAX_AMOUNT`: **Total tax** (item + bill-level) ← **Now using this**

**Updated Calculations**:
```sql
-- Before (incorrect - only subtracting item-level tax)
NetAmount = (double_sub_total - double_mgst_tax_amount)

-- After (correct - subtracting total tax)
NetAmount = (double_sub_total - double_total_tax_amount)
```

### 📝 Documentation Tasks
- [x] Update ETL README with DOUBLE_TOTAL_TAX_AMOUNT field description
- [x] Update CHANGELOG with tax fix details
- [ ] Update Notion: Staging tables schema documentation
  - Add double_total_tax_amount field to staging_sales_items
- [ ] Update Notion: ETL mapping documentation
  - Document use of DOUBLE_TOTAL_TAX_AMOUNT vs double_mgst_tax_amount

---

## [1.5.0] - 2025-10-15

### ✨ Added - SUBSALES_TYPE Support
- **ETL Enhancement**: Extract SUBSALES_TYPE from APP_4_SALES
  - Added SUBSALES_TYPE column to staging_sales table
  - Added SubSalesType column to fact_sales_transactions table
  - Updated transform_sales_facts.sql to map SUBSALES_TYPE
  - Enables separation of catering orders from regular take-away

### 📊 Schema Changes
- `dbo.staging_sales`: Added SUBSALES_TYPE VARCHAR(100)
- `dbo.fact_sales_transactions`: Added SubSalesType VARCHAR(100)

### 🔧 Migration Scripts
- `alter_staging_sales_add_subsales_type.sql`
- `alter_fact_sales_add_subsales_type.sql`

### 💡 Impact
- Warehouse can now distinguish "Catering (Outdoor)" from "Take Away"
- Supports detailed sales reporting by sale sub-type
- Enables accurate replication of Xilnex portal's detailed daily sales report

### 📝 Documentation Tasks
- [ ] Update Notion: Database schema documentation
  - Add SubSalesType to fact_sales_transactions schema
  - Add SUBSALES_TYPE to staging_sales schema
- [ ] Update Notion: ETL mapping documentation
  - Document APP_4_SALES.SUBSALES_TYPE extraction
  - Document catering reclassification logic

---

## [1.3.0] - 2025-10-14

### ✨ Added - Parquet Export System
- **Azure SQL Parquet Export Scripts**: Client-side Python scripts for bulk data export testing
  - `config.py`: Environment-based configuration with `.env` support
  - `export_to_parquet.py`: Direct Azure SQL → Parquet export with chunked processing
  - `validate_parquet.py`: Parquet file validation and inspection
  - `test_azure_connection.py`: Connection testing and record count verification
  - `PARQUET_EXPORT_GUIDE.md`: Complete usage documentation
  
- **Export Features**:
  - Direct query-to-Parquet conversion (no intermediate CSV files)
  - Chunked reading (50K rows/chunk) for memory efficiency
  - Snappy compression (3-5x file size reduction)
  - Progress tracking during export
  - Comprehensive validation with data type inspection
  
- **Performance Results** (October 2025 - 14 days):
  - Sales: 664,382 rows → 9.38 MB (47x compression, 16.32s)
  - Sales Items: 3,487,947 rows → 44.60 MB (24x compression, 81.74s)
  - Payments: 659,995 rows → 7.58 MB (18x compression, 14.37s)
  - **Total: 4.8M rows in under 2 minutes**
  
- **Strategic Value**:
  - Validates bulk export approach for historical data migration
  - Tests CDC strategy before full implementation
  - Provides comparison baseline vs. current Python ETL
  - Enables efficient data archival and backup

### 🔧 Changed
- Updated `requirements.txt` to include `pyarrow==19.0.0` for Parquet support
- Configuration now uses environment variables via `python-dotenv`

---

## [1.2.0] - 2025-10-14

### 🔧 Critical Fixes
- **Multi-Payment Duplication Fix**: Resolved cartesian join issue causing duplicated fact rows
  - Added `PaymentDedup` and `PaymentPerInvoice` CTEs in `transform_sales_facts.sql`
  - Prevents multiple payment records from multiplying sales line items
  - Selects single representative payment per sale (prioritizing non-empty card types)
  - **Impact**: Fixed ~3x data inflation for multi-payment transactions
  
- **Sales Status Architecture**: Implemented flexible status filtering system
  - Added `SalesStatus` column to `fact_sales_transactions` table
  - Extracts ALL transaction statuses (COMPLETED, CANCELLED, etc.) from source
  - Stores all statuses in warehouse for maximum flexibility
  - Applies status filtering at API level (defaults to COMPLETED to match Xilnex portal)
  - **Impact**: Enables frontend to choose which statuses to include in reports

- **Tax-Inclusive Reporting**: Updated API to match Xilnex portal amounts
  - Changed Daily Sales Report from `NetAmount` to `TotalAmount`
  - Sales amounts now include tax for consistency with Xilnex portal
  - **Impact**: Portal amounts now match Xilnex portal (e.g., RM 3,090.20 vs RM 2,938.62)

### ✨ Added
- **New SQL Script**: `alter_staging_sales_add_sales_status.sql`
  - Adds `SALES_STATUS` column to `staging_sales` table
- **New SQL Script**: `alter_fact_sales_add_sales_status.sql`
  - Adds `SalesStatus` column to `fact_sales_transactions` table
  
### 📚 Documentation
- Updated `README.md` with status filtering and tax-inclusive amount notes
- Updated `docs/DATABASE_SCHEMA.md` with payment field clarifications
- Updated API documentation in `marrybrown_api/README.md`
- Enhanced Daily Sales endpoint documentation with behavioral notes

### 🎯 Impact
- **Data Accuracy**: Eliminated multi-payment transaction duplication (critical fix)
- **Portal Alignment**: Sales amounts now match Xilnex portal exactly
- **Reporting Consistency**: All reports use same transaction scope as Xilnex

---

## [1.1.0] - 2025-10-09

### ✨ Added
- **Payment Type Integration**: Full payment method tracking in fact table
  - Added `PaymentTypeKey` foreign key to `fact_sales_transactions`
  - Extract payment data from `APP_4_PAYMENT` table
  - New staging table: `staging_payments`
  - Payment data transformation in `transform_sales_facts.sql`
  - Payment category aggregation (Cash, Card, E-Wallet, Voucher, Other)

### 🔧 Fixed
- **Threading Issue**: Fixed `etl_fact_sales_historical.py` missing parameters
  - Engines now created once and passed to threads
  - Added `total_payment_rows` variable initialization
  - Improved performance by reducing redundant engine creation

### 📚 Documentation
- Updated `DATABASE_SCHEMA.md` with `PaymentTypeKey` column
- Updated `docs/QUICKSTART.md` with payment testing queries
- Updated `README.md` with payment data flow
- Created `PAYMENT_TYPE_ETL_FIX.md` for technical details
- Created `add_payment_type_key.sql` schema update script
- Created `DOCUMENTATION_UPDATES.md` summary

### 🎯 Impact
- EOD Summary Report now shows real payment breakdown data
- Enhanced analytics capabilities for payment method analysis
- Complete end-to-end payment tracking from source to reporting

---

## [1.0.0] - 2025-10-01

### 🎉 Initial Release
- **ETL Pipeline**: Complete Python-based ETL for all dimension tables
- **ELT Pipeline**: Multithreaded extraction for fact sales data
- **Star Schema**: 8 dimension tables + 1 fact table
- **Dimensions**:
  - `dim_date` (4,018 rows - calendar)
  - `dim_time` (86,400 rows - time of day)
  - `dim_products` (~2,000 rows)
  - `dim_customers` (~100,000 rows)
  - `dim_locations` (~200 rows)
  - `dim_staff` (~1,000 rows)
  - `dim_promotions` (~500 rows)
  - `dim_payment_types` (~10 rows)
- **Fact Table**: `fact_sales_transactions` (millions of rows)
- **Performance**: 7 days of data processed in 5-10 minutes
- **Multithreading**: 4 parallel workers for concurrent processing
- **Chunked Processing**: 20K rows per chunk for memory efficiency

### 📚 Documentation
- Comprehensive `README.md`
- `DATABASE_SCHEMA.md` with full star schema reference
- `docs/QUICKSTART.md` for 10-minute setup
- `docs/PROJECT_CONTEXT.md` for business context

---

## Version History

| Version | Date | Description |
|---------|------|-------------|
| 1.2.0 | 2025-10-14 | Critical fixes: multi-payment deduplication, status filtering, tax-inclusive reporting |
| 1.1.0 | 2025-10-09 | Payment type integration |
| 1.0.0 | 2025-10-01 | Initial ETL pipeline release |

---

## Upcoming Features

### 🔮 Planned for v1.2.0
- [ ] Automated scheduling with Apache Airflow
- [ ] Change Data Capture (CDC) for real-time updates
- [ ] Data quality validation scripts
- [ ] Error handling and retry logic
- [ ] Email alerts on pipeline failures
- [ ] Bulk CSV import for historical data

### 🔮 Planned for v2.0.0
- [ ] Incremental updates (only new/changed data)
- [ ] Data lineage tracking
- [ ] Automated testing suite
- [ ] Performance monitoring dashboard
- [ ] Multi-environment support (dev/staging/prod)

---

## Breaking Changes

### v1.2.0
- **Schema Change**: `staging_sales` table now includes `SALES_STATUS` column
  - **Action Required**: Run `alter_staging_sales_add_sales_status.sql` before running ETL
  - **Impact**: ETL now extracts ALL transaction statuses (not just COMPLETED)
  - **Migration**: Rerun ETL for affected date ranges to include all statuses

- **Schema Change**: `fact_sales_transactions` table now includes `SalesStatus` column
  - **Action Required**: Run `alter_fact_sales_add_sales_status.sql` before running transform
  - **Impact**: Fact table now stores transaction status for each row
  - **Migration**: Rerun transform to populate status column

- **API Behavior Change**: Daily Sales Report now filters by status and returns tax-inclusive amounts
  - **Impact**: API filters to `SalesStatus = 'COMPLETED'` by default (matching Xilnex portal)
  - **Impact**: `sales_amount` values include tax (TotalAmount instead of NetAmount)
  - **Migration**: Frontend/reports automatically reflect new behavior (no code changes needed)

### v1.1.0
- **Schema Change**: `fact_sales_transactions` now requires `PaymentTypeKey` column
  - **Action Required**: Run `add_payment_type_key.sql` before running ETL
  - **Impact**: Existing queries joining to `dim_payment_types` will now work
  - **Migration**: No data migration needed for existing rows (defaults to -1)

---

## Notes

- All version numbers follow [Semantic Versioning](https://semver.org/)
- **MAJOR**: Breaking changes requiring schema updates
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

---

**Last Updated**: October 14, 2025  
**Maintained By**: YONG WERN JIE A22EC0121

