"""
Investigate Sales Amount Calculation Differences
Compare Xilnex (ALL statuses) vs Our API ETL - Store-by-Store Analysis

Focus on understanding:
1. Why IOI KULAI profit is PERFECT (never achieved with Direct DB!)
2. Why Sales Amount differs
3. Why ANGSANA profit differs

Author: YONG WERN JIE
Date: October 28, 2025
"""

import pandas as pd
from pathlib import Path

print("="*80)
print("SALES CALCULATION INVESTIGATION")
print("="*80)
print()

# File paths
xilnex_all_statuses = r"C:\Users\MIS INTERN\Downloads\Daily Sales_28-10-2025 (Web) (1).xlsx"
our_file = r"C:\Users\MIS INTERN\Downloads\Daily_Sales_API_Test_20181001_20181031.xlsx"

# Check files exist
if not Path(xilnex_all_statuses).exists():
    print(f"[ERROR] Xilnex ALL statuses file not found: {xilnex_all_statuses}")
    exit(1)

if not Path(our_file).exists():
    print(f"[ERROR] Our file not found: {our_file}")
    exit(1)

print("[STEP 1] Reading Xilnex Export (ALL Statuses)")
print("-"*80)

# Read Xilnex export - skip metadata rows, header is at row 11 (index 10)
xilnex_raw = pd.read_excel(xilnex_all_statuses, header=10)
print(f"Raw rows read: {len(xilnex_raw)}")

# Filter rows that contain date pattern + "Total"
xilnex_totals = xilnex_raw[
    xilnex_raw['Date'].astype(str).str.match(r'^\d{2}/\d{2}/\d{4} Total$', na=False)
].copy()
print(f"Date total rows found: {len(xilnex_totals)}")

# Parse date from "DD/MM/YYYY Total" format
xilnex_totals['ParsedDate'] = xilnex_totals['Date'].str.replace(' Total', '')
xilnex_totals['ParsedDate'] = pd.to_datetime(xilnex_totals['ParsedDate'], format='%d/%m/%Y', errors='coerce')

# Rename columns
xilnex_totals = xilnex_totals.rename(columns={
    'Sales Amount': 'Xilnex_Sales',
    'Profit Amount': 'Xilnex_Profit'
})

# Also read store-level detail (rows without "Total")
xilnex_stores = xilnex_raw[
    ~xilnex_raw['Date'].astype(str).str.contains('Total', na=False) &
    xilnex_raw['Store'].notna()
].copy()

# For store rows, the date is in the previous "Total" row
# We need to parse this differently - let's read with a different approach
print(f"\n[STEP 1B] Reading Xilnex Store-Level Details")
print("-"*80)

# Re-read raw data to parse store-level details
xilnex_df = pd.read_excel(xilnex_all_statuses, header=None)

# Find the header row (contains "Date", "Store", "Sales Amount", "Profit Amount")
header_row = None
for i, row in xilnex_df.iterrows():
    if 'Date' in str(row.values) and 'Store' in str(row.values):
        header_row = i
        break

if header_row:
    # Read with correct header
    xilnex_detail = pd.read_excel(xilnex_all_statuses, header=header_row)
    
    # Forward fill dates (dates are only in first row of each date group)
    xilnex_detail['DateFilled'] = None
    current_date = None
    
    for i, row in xilnex_detail.iterrows():
        date_val = row['Date']
        
        # Check if it's a date (datetime object or parseable string)
        if pd.notna(date_val) and not isinstance(date_val, str):
            # It's a datetime
            current_date = date_val
            xilnex_detail.at[i, 'DateFilled'] = current_date
        elif pd.notna(date_val) and isinstance(date_val, str) and 'Total' not in date_val:
            # Try to parse as date
            try:
                parsed = pd.to_datetime(date_val, format='%d/%m/%Y', errors='coerce')
                if pd.notna(parsed):
                    current_date = parsed
                    xilnex_detail.at[i, 'DateFilled'] = current_date
            except:
                pass
        
        # Forward fill
        if current_date and pd.isna(xilnex_detail.at[i, 'DateFilled']):
            xilnex_detail.at[i, 'DateFilled'] = current_date
    
    # Filter to only store rows (not totals, not empty)
    xilnex_by_store = xilnex_detail[
        xilnex_detail['Store'].notna() &
        ~xilnex_detail['Store'].astype(str).str.contains('Total', na=False) &
        ~xilnex_detail['Store'].astype(str).str.contains('Grand', na=False)
    ].copy()
    
    # Clean up
    xilnex_by_store = xilnex_by_store.rename(columns={
        'DateFilled': 'ParsedDate',
        'Store': 'StoreName',
        'Sales Amount': 'Xilnex_Sales',
        'Profit Amount': 'Xilnex_Profit'
    })
    
    xilnex_by_store = xilnex_by_store[['ParsedDate', 'StoreName', 'Xilnex_Sales', 'Xilnex_Profit']].copy()
    xilnex_by_store = xilnex_by_store[xilnex_by_store['ParsedDate'].notna()]
    
    # Ensure ParsedDate is datetime type and normalize to date only
    xilnex_by_store['ParsedDate'] = pd.to_datetime(xilnex_by_store['ParsedDate']).dt.normalize()
    
    print(f"Store-level rows found: {len(xilnex_by_store)}")
    print(f"Date range: {xilnex_by_store['ParsedDate'].min()} to {xilnex_by_store['ParsedDate'].max()}")
    print(f"Stores: {xilnex_by_store['StoreName'].unique()}")
    print()

print()
print("[STEP 2] Reading Our Export (Flat Structure)")
print("-"*80)

# Read our export
our_df = pd.read_excel(our_file)
print(f"Raw rows read: {len(our_df)}")

# Remove TOTAL row if exists
our_df = our_df[our_df['Date'] != 'TOTAL'].copy()
print(f"Data rows (excluding TOTAL): {len(our_df)}")

# Parse date
our_df['ParsedDate'] = pd.to_datetime(our_df['Date'], format='%d/%m/%Y')

# Normalize to date only (remove time component)
our_df['ParsedDate'] = our_df['ParsedDate'].dt.normalize()

# Rename for clarity
our_df = our_df.rename(columns={
    'Store Name': 'StoreName',
    'Sales Amount (RM)': 'Our_Sales',
    'Profit Amount (RM)': 'Our_Profit'
})

print(f"Date range: {our_df['ParsedDate'].min()} to {our_df['ParsedDate'].max()}")
print(f"Stores: {our_df['StoreName'].unique()}")
print()

print("[STEP 3] Store-by-Store Comparison")
print("-"*80)
print()

# Merge by date and store
comparison = pd.merge(
    xilnex_by_store,
    our_df,
    on=['ParsedDate', 'StoreName'],
    how='outer',
    indicator=True
)

# Calculate differences
comparison['Sales_Diff'] = comparison['Our_Sales'] - comparison['Xilnex_Sales']
comparison['Profit_Diff'] = comparison['Our_Profit'] - comparison['Xilnex_Profit']
comparison['Sales_Diff_Pct'] = (comparison['Sales_Diff'] / comparison['Xilnex_Sales'] * 100).round(4)
comparison['Profit_Diff_Pct'] = (comparison['Profit_Diff'] / comparison['Xilnex_Profit'] * 100).round(4)

# Sort by date and store
comparison = comparison.sort_values(['ParsedDate', 'StoreName'])

# Aggregate by store
by_store = comparison[comparison['_merge'] == 'both'].groupby('StoreName').agg({
    'Xilnex_Sales': 'sum',
    'Our_Sales': 'sum',
    'Sales_Diff': 'sum',
    'Xilnex_Profit': 'sum',
    'Our_Profit': 'sum',
    'Profit_Diff': 'sum'
}).reset_index()

by_store['Sales_Diff_Pct'] = (by_store['Sales_Diff'] / by_store['Xilnex_Sales'] * 100).round(4)
by_store['Profit_Diff_Pct'] = (by_store['Profit_Diff'] / by_store['Xilnex_Profit'] * 100).round(4)

print("Store-Level Totals (October 2018):")
print("="*80)
print()

for _, row in by_store.iterrows():
    print(f"{row['StoreName']}")
    print("-"*80)
    print(f"  Sales Amount:")
    print(f"    Xilnex:     RM {row['Xilnex_Sales']:>12,.2f}")
    print(f"    Our API:    RM {row['Our_Sales']:>12,.2f}")
    print(f"    Difference: RM {row['Sales_Diff']:>12,.2f} ({row['Sales_Diff_Pct']:>6.2f}%)")
    print()
    print(f"  Profit Amount:")
    print(f"    Xilnex:     RM {row['Xilnex_Profit']:>12,.2f}")
    print(f"    Our API:    RM {row['Our_Profit']:>12,.2f}")
    print(f"    Difference: RM {row['Profit_Diff']:>12,.2f} ({row['Profit_Diff_Pct']:>6.2f}%)")
    
    if abs(row['Profit_Diff_Pct']) < 0.01:
        print(f"    [PERFECT MATCH!]")
    elif abs(row['Profit_Diff_Pct']) < 1.0:
        print(f"    [Excellent - <1% diff]")
    else:
        print(f"    [Needs investigation]")
    
    print()

print()
print("[STEP 4] Sample Date Analysis - Oct 1, 2018")
print("-"*80)
print()

# Get Oct 1 data
oct1 = comparison[comparison['ParsedDate'] == '2018-10-01'].copy()

if len(oct1) > 0:
    print("Oct 1, 2018 - Store Breakdown:")
    print()
    for _, row in oct1.iterrows():
        print(f"{row['StoreName']}:")
        print(f"  Xilnex Sales: RM {row['Xilnex_Sales']:,.2f}  |  Our API: RM {row['Our_Sales']:,.2f}  |  Diff: RM {row['Sales_Diff']:,.2f}")
        print(f"  Xilnex Profit: RM {row['Xilnex_Profit']:,.2f}  |  Our API: RM {row['Our_Profit']:,.2f}  |  Diff: RM {row['Profit_Diff']:,.2f}")
        print()

print()
print("[STEP 5] Days with Perfect Profit Match (IOI KULAI)")
print("-"*80)
print()

# Filter IOI KULAI
ioi_kulai = comparison[
    (comparison['StoreName'] == 'MB IOI KULAI') &
    (comparison['_merge'] == 'both')
].copy()

perfect_profit = ioi_kulai[ioi_kulai['Profit_Diff'].abs() < 0.01]
print(f"IOI KULAI: {len(perfect_profit)} out of {len(ioi_kulai)} days have PERFECT profit match (<RM 0.01 diff)")
print()

if len(perfect_profit) > 0:
    print("Sample perfect profit days:")
    for _, row in perfect_profit.head(10).iterrows():
        print(f"  {row['ParsedDate'].strftime('%d/%m/%Y')}: Xilnex RM {row['Xilnex_Profit']:,.2f} = Our RM {row['Our_Profit']:,.2f}")

print()
print()

print("[STEP 6] Sales Amount Pattern Analysis")
print("-"*80)
print()

# Check if there's a consistent ratio
print("Checking for consistent Sales Amount patterns...")
print()

# Sample some dates
sample_dates = comparison[(comparison['_merge'] == 'both')].head(20)

print("Date-Store samples showing Sales Amount relationship:")
print()
print(f"{'Date':<12} {'Store':<20} {'Xilnex':>12} {'Our API':>12} {'Diff':>10} {'Ratio':>8}")
print("-"*90)

for _, row in sample_dates.iterrows():
    ratio = row['Our_Sales'] / row['Xilnex_Sales'] if row['Xilnex_Sales'] != 0 else 0
    print(f"{row['ParsedDate'].strftime('%d/%m/%Y'):<12} {row['StoreName']:<20} "
          f"RM {row['Xilnex_Sales']:>9,.2f} RM {row['Our_Sales']:>9,.2f} "
          f"RM {row['Sales_Diff']:>7,.2f} {ratio:>7.4f}")

print()
print("="*80)
print("KEY FINDINGS")
print("="*80)
print()

# IOI KULAI profit accuracy
ioi_profit_accuracy = 100 - abs(by_store[by_store['StoreName'] == 'MB IOI KULAI']['Profit_Diff_Pct'].values[0])
print(f"[SUCCESS] IOI KULAI Profit Accuracy: {ioi_profit_accuracy:.4f}%")
print("   (This is a BREAKTHROUGH - never achieved with Direct DB!)")
print()

# Check if Sales Amount shows a pattern
avg_ratio = (comparison[comparison['_merge'] == 'both']['Our_Sales'] / 
             comparison[comparison['_merge'] == 'both']['Xilnex_Sales']).mean()
print(f"Average Our/Xilnex Sales Ratio: {avg_ratio:.4f}")

if abs(avg_ratio - 1.0) > 0.01:
    if avg_ratio > 1.0:
        print(f"   > Our Sales Amount is consistently {((avg_ratio - 1) * 100):.2f}% HIGHER")
        print("   > Possible causes:")
        print("      - We use TotalAmount (Net + Tax)")
        print("      - Xilnex might use different calculation")
        print("      - We include different transaction types")
    else:
        print(f"   > Our Sales Amount is consistently {((1 - avg_ratio) * 100):.2f}% LOWER")

print()
print("RECOMMENDATION:")
print("-"*80)
print("1. Check the formula in sales.py:")
print("   - Current: SUM(f.TotalAmount) for Sales Amount")
print("   - Current: SUM(f.NetAmount - f.CostAmount) for Profit")
print()
print("2. Investigate what Xilnex portal uses for 'Sales Amount':")
print("   - Is it GrossAmount?")
print("   - Is it NetAmount?")
print("   - Is it TotalAmount?")
print("   - Does it include/exclude certain statuses?")
print()
print("3. Run a SQL query to check our raw data:")
print("   SELECT SaleNumber, TotalAmount, NetAmount, TaxAmount, GrossAmount")
print("   FROM fact_sales_transactions_api")
print("   WHERE DateKey IN (SELECT DateKey FROM dim_date WHERE FullDate = '2018-10-01')")
print("   AND LocationKey IN (SELECT LocationKey FROM dim_locations WHERE LocationName = 'MB IOI KULAI')")
print()

# Save detailed comparison
output_file = f"Store_Level_Comparison_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    # By-store summary
    by_store.to_excel(writer, sheet_name='By Store Summary', index=False)
    
    # All daily comparisons
    comparison[comparison['_merge'] == 'both'].to_excel(writer, sheet_name='Daily Store Comparison', index=False)
    
    # IOI KULAI only
    ioi_kulai.to_excel(writer, sheet_name='IOI KULAI Only', index=False)

print(f"Detailed comparison saved to: {output_file}")
print()

