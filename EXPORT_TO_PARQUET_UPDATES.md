# Export to Parquet Updates - October 23, 2025

## Changes Made

### 1. Configuration Update (`config.py`)
- **Changed**: `MONTH_TO_EXPORT` from `'2025-10'` to `'2025-09'`
- **Purpose**: Export September 2025 data instead of October

### 2. Sales Table Export (`export_to_parquet.py` - Lines 84-104)
- **Added Field**: `SUBSALES_TYPE` (line 96)
- **Purpose**: Required for Product Mix report's enhanced Sale Type classification
- **Impact**: Enables proper grouping by SubSalesType (e.g., FoodPanda, GrabFood)

### 3. Sales Items Table Export (`export_to_parquet.py` - Lines 112-130)
- **Added Field**: `DOUBLE_TOTAL_TAX_AMOUNT` (line 123)
- **Purpose**: Required for accurate tax calculations in transformation
- **Impact**: Fixes tax amount discrepancies in fact table

### 4. New Recipe Costs Export (`export_to_parquet.py` - Lines 67-112)
- **New Function**: `export_recipe_costs()`
- **Purpose**: Pre-calculate costs for combo items from recipe ingredients
- **Query**:
  ```sql
  SELECT 
      i.ID as item_id,
      rs.SALES_TYPE as sales_type,
      ISNULL(SUM(ISNULL(rs.DOUBLE_QUANTITY, 0) * ISNULL(rs.DOUBLE_COST, 0)), 0) as calculated_recipe_cost,
      COUNT(rs.RM_ITEM_ID) as ingredient_count
  FROM COM_5013.APP_4_ITEM i
  JOIN COM_5013.APP_4_RECIPESUMMARY rs ON rs.ITEM_ID = i.ID
  WHERE i.BOOL_ISPACKAGE = 1
    AND rs.DOUBLE_COST IS NOT NULL
    AND rs.DOUBLE_QUANTITY IS NOT NULL
  GROUP BY i.ID, rs.SALES_TYPE
  ```
- **Output**: `recipe_costs.parquet`
- **Impact**: Fixes abnormal cost values for combo items (e.g., Cheesy Burger Combo)

### 5. Main Function Update (`export_to_parquet.py` - Lines 203-213)
- **Added**: Call to `export_recipe_costs()`
- **Updated**: Export summary to include recipe costs count
- **Impact**: Complete export now includes all 4 required data files

---

## Export Output Files

The updated script will now export 4 Parquet files:

| File | Purpose | Date-Specific |
|------|---------|---------------|
| `sales_202509.parquet` | Sales headers with SUBSALES_TYPE | ✅ Yes (Sept 2025) |
| `sales_items_202509.parquet` | Sales items with DOUBLE_TOTAL_TAX_AMOUNT | ✅ Yes (Sept 2025) |
| `payments_202509.parquet` | Payment records | ✅ Yes (Sept 2025) |
| `recipe_costs.parquet` | Pre-calculated combo item costs | ❌ No (all recipes) |

---

## How to Run

### Step 1: Ensure Connection is Stable
Wait for your IP address to be stable before running the export.

### Step 2: Run Export Script
```bash
cd C:\Users\MIS INTERN\marrybrown_etl
python export_to_parquet.py
```

### Step 3: Expected Output
```
Exporting data for 2025-09
Date range: 2025-09-01 to 2025-10-01
Export directory: C:/exports
============================================================

Exporting to sales_202509.parquet...
  Processed 150,000 rows...
  ✓ Exported 150,234 rows in 18.45 seconds
  ✓ File size: 4.23 MB
  ✓ Compression ratio: 42x

Exporting to sales_items_202509.parquet...
  Processed 750,000 rows...
  ✓ Exported 789,456 rows in 92.34 seconds
  ✓ File size: 19.87 MB
  ✓ Compression ratio: 26x

Exporting to payments_202509.parquet...
  Processed 150,000 rows...
  ✓ Exported 149,876 rows in 16.23 seconds
  ✓ File size: 3.45 MB
  ✓ Compression ratio: 19x

Exporting recipe costs for combo items...
  ✓ Exported 11,630 recipe cost records in 5.67 seconds
  ✓ File size: 0.38 MB

============================================================
Export Summary:
  Sales: 150,234 rows
  Sales Items: 789,456 rows
  Payments: 149,876 rows
  Recipe Costs: 11,630 rows

  Total Exported: 1,101,196 rows

Export complete!
```

---

## Next Steps

After successful export:

1. **Verify Files Created**:
   ```powershell
   dir C:\exports\*.parquet
   ```

2. **Update ETL to Read from Parquet** (Next Phase):
   - Modify `etl_fact_sales_historical.py` to read from Parquet files
   - Test extraction from Parquet to staging tables
   - Run transformation script

3. **Validate Data**:
   - Compare row counts between Xilnex and exported Parquet
   - Spot-check sample records for data integrity
   - Verify recipe costs match expected values

---

## Troubleshooting

### If Export Fails Mid-Way (IP Swap):
- Script can be re-run safely - will overwrite partial files
- Export is idempotent - no duplicate data

### If Recipe Costs Return 0 Rows:
- Check `APP_4_RECIPESUMMARY` table has data
- Verify `BOOL_ISPACKAGE = 1` items exist in `APP_4_ITEM`
- Check for NULL costs in ingredients

### If Connection Times Out:
- Increase chunk size: `chunk_size=100000` for faster export
- Run during off-peak hours (late night)
- Export one table at a time if needed

---

## Data Quality Checks

After export, run these checks:

```python
import pandas as pd

# Check sales
sales = pd.read_parquet('C:/exports/sales_202509.parquet')
print(f"Sales rows: {len(sales)}")
print(f"Date range: {sales['datetime__sales_date'].min()} to {sales['datetime__sales_date'].max()}")
print(f"SUBSALES_TYPE null count: {sales['SUBSALES_TYPE'].isna().sum()}")

# Check sales items
items = pd.read_parquet('C:/exports/sales_items_202509.parquet')
print(f"\nSales Items rows: {len(items)}")
print(f"DOUBLE_TOTAL_TAX_AMOUNT null count: {items['DOUBLE_TOTAL_TAX_AMOUNT'].isna().sum()}")

# Check recipe costs
recipes = pd.read_parquet('C:/exports/recipe_costs.parquet')
print(f"\nRecipe Costs rows: {len(recipes)}")
print(f"Unique items: {recipes['item_id'].nunique()}")
print(f"Average cost: RM {recipes['calculated_recipe_cost'].mean():.2f}")
```

---

## Version History

- **v1.4.0** (Oct 23, 2025): Added SUBSALES_TYPE, DOUBLE_TOTAL_TAX_AMOUNT, and recipe costs export
- **v1.3.0** (Oct 14, 2025): Initial Parquet export system

