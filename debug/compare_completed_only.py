"""
Compare API ETL Excel Export vs Xilnex Portal Export
COMPLETED STATUS ONLY - Apples-to-apples comparison

Author: YONG WERN JIE
Date: October 28, 2025
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

print("="*80)
print("API ETL vs XILNEX PORTAL - COMPLETED STATUS COMPARISON")
print("="*80)
print()

# File paths
our_file = r"C:\Users\MIS INTERN\Downloads\Daily_Sales_API_Test_20181001_20181031 (1).xlsx"
xilnex_file = r"C:\Users\MIS INTERN\Downloads\Daily Sales_28-10-2025 (Web) (2).xlsx"

# Check files exist
if not Path(our_file).exists():
    print(f"[ERROR] Our file not found: {our_file}")
    exit(1)

if not Path(xilnex_file).exists():
    print(f"[ERROR] Xilnex file not found: {xilnex_file}")
    exit(1)

print("[STEP 1] Reading Xilnex Export (Completed Status)")
print("-"*80)

# Read Xilnex export - skip metadata rows, header is at row 11 (index 10)
xilnex_raw = pd.read_excel(xilnex_file, header=10)
print(f"Raw rows read: {len(xilnex_raw)}")

# Filter rows that contain date pattern + "Total" (e.g., "01/10/2018 Total")
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

# Keep only needed columns
xilnex_totals = xilnex_totals[['ParsedDate', 'Xilnex_Sales', 'Xilnex_Profit']].copy()
xilnex_totals = xilnex_totals.sort_values('ParsedDate')

print(f"  Xilnex Date Range: {xilnex_totals['ParsedDate'].min()} to {xilnex_totals['ParsedDate'].max()}")
print(f"  Total Sales Amount: RM {xilnex_totals['Xilnex_Sales'].sum():,.2f}")
print(f"  Total Profit Amount: RM {xilnex_totals['Xilnex_Profit'].sum():,.2f}")
print()

print("[STEP 2] Reading Our Export (API ETL - Completed Status)")
print("-"*80)

# Read our export
our_df = pd.read_excel(our_file)
print(f"Raw rows read: {len(our_df)}")

# Remove TOTAL row if exists
our_df = our_df[our_df['Date'] != 'TOTAL'].copy()
print(f"Data rows (excluding TOTAL): {len(our_df)}")

# Parse date
our_df['ParsedDate'] = pd.to_datetime(our_df['Date'], format='%d/%m/%Y')
our_df['ParsedDate'] = our_df['ParsedDate'].dt.normalize()

# Group by date and sum
our_totals = our_df.groupby('ParsedDate').agg({
    'Sales Amount (RM)': 'sum',
    'Profit Amount (RM)': 'sum'
}).reset_index()

our_totals = our_totals.rename(columns={
    'Sales Amount (RM)': 'Our_Sales',
    'Profit Amount (RM)': 'Our_Profit'
})

our_totals = our_totals.sort_values('ParsedDate')

print(f"  Our Date Range: {our_totals['ParsedDate'].min()} to {our_totals['ParsedDate'].max()}")
print(f"  Total Sales Amount: RM {our_totals['Our_Sales'].sum():,.2f}")
print(f"  Total Profit Amount: RM {our_totals['Our_Profit'].sum():,.2f}")
print()

print("[STEP 3] Merging and Comparing")
print("-"*80)

# Merge on date
comparison = pd.merge(
    xilnex_totals,
    our_totals,
    on='ParsedDate',
    how='outer',
    indicator=True
)

# Calculate differences
comparison['Sales_Diff'] = comparison['Our_Sales'] - comparison['Xilnex_Sales']
comparison['Profit_Diff'] = comparison['Our_Profit'] - comparison['Xilnex_Profit']
comparison['Sales_Diff_Pct'] = (comparison['Sales_Diff'] / comparison['Xilnex_Sales'] * 100).round(4)
comparison['Profit_Diff_Pct'] = (comparison['Profit_Diff'] / comparison['Xilnex_Profit'] * 100).round(4)

# Sort by date
comparison = comparison.sort_values('ParsedDate')

print(f"Total dates compared: {len(comparison)}")
print()

# Check for missing dates
only_xilnex = comparison[comparison['_merge'] == 'left_only']
only_ours = comparison[comparison['_merge'] == 'right_only']

if len(only_xilnex) > 0:
    print(f"[!] Dates only in Xilnex: {len(only_xilnex)}")
    print(only_xilnex[['ParsedDate', 'Xilnex_Sales']])
    print()

if len(only_ours) > 0:
    print(f"[!] Dates only in Our export: {len(only_ours)}")
    print(only_ours[['ParsedDate', 'Our_Sales']])
    print()

# Compare matching dates
matching = comparison[comparison['_merge'] == 'both'].copy()
print(f"Matching dates: {len(matching)}")
print()

print("[STEP 4] Detailed Comparison by Date")
print("-"*80)
print()

# Format for display
matching['Date'] = matching['ParsedDate'].dt.strftime('%d/%m/%Y')
matching['Xilnex Sales'] = matching['Xilnex_Sales'].apply(lambda x: f"RM {x:,.2f}")
matching['Our Sales'] = matching['Our_Sales'].apply(lambda x: f"RM {x:,.2f}")
matching['Diff'] = matching['Sales_Diff'].apply(lambda x: f"RM {x:,.2f}")
matching['Diff %'] = matching['Sales_Diff_Pct'].apply(lambda x: f"{x:.4f}%")

# Display comparison table
display_cols = ['Date', 'Xilnex Sales', 'Our Sales', 'Diff', 'Diff %']
print(matching[display_cols].to_string(index=False))
print()

print("="*80)
print("SUMMARY - COMPLETED STATUS ONLY")
print("="*80)
print()

# Overall totals
xilnex_total_sales = matching['Xilnex_Sales'].sum()
our_total_sales = matching['Our_Sales'].sum()
total_diff = our_total_sales - xilnex_total_sales
total_diff_pct = (total_diff / xilnex_total_sales * 100) if xilnex_total_sales > 0 else 0

xilnex_total_profit = matching['Xilnex_Profit'].sum()
our_total_profit = matching['Our_Profit'].sum()
profit_diff = our_total_profit - xilnex_total_profit
profit_diff_pct = (profit_diff / xilnex_total_profit * 100) if xilnex_total_profit > 0 else 0

print(f"Sales Amount:")
print(f"  Xilnex Total (Completed):  RM {xilnex_total_sales:,.2f}")
print(f"  Our Total (Completed):     RM {our_total_sales:,.2f}")
print(f"  Difference:                RM {total_diff:,.2f} ({total_diff_pct:.4f}%)")
print()

print(f"Profit Amount:")
print(f"  Xilnex Total (Completed):  RM {xilnex_total_profit:,.2f}")
print(f"  Our Total (Completed):     RM {our_total_profit:,.2f}")
print(f"  Difference:                RM {profit_diff:,.2f} ({profit_diff_pct:.4f}%)")
print()

# Accuracy calculation
sales_accuracy = 100 - abs(total_diff_pct)
profit_accuracy = 100 - abs(profit_diff_pct)

print(f"Accuracy:")
print(f"  Sales:  {sales_accuracy:.4f}%")
print(f"  Profit: {profit_accuracy:.4f}%")
print()

# Success criteria check
success_threshold = 99.97
if sales_accuracy >= success_threshold and profit_accuracy >= success_threshold:
    print(f"[SUCCESS] Both accuracies >= {success_threshold}%")
    print("   API ETL with COMPLETED status filter is production-ready!")
else:
    print(f"[INFO] Accuracy: {sales_accuracy:.4f}% (Target: {success_threshold}%)")
    if sales_accuracy >= 99.0:
        print("   Still excellent accuracy! Very close to target.")

print()

# Statistics on differences
print("Difference Statistics (by date):")
print(f"  Max Sales Difference:  RM {matching['Sales_Diff'].abs().max():,.2f}")
print(f"  Avg Sales Difference:  RM {matching['Sales_Diff'].abs().mean():,.2f}")
print(f"  Max Profit Difference: RM {matching['Profit_Diff'].abs().max():,.2f}")
print(f"  Avg Profit Difference: RM {matching['Profit_Diff'].abs().mean():,.2f}")
print()

# Days with significant differences (>1%)
significant_diff = matching[matching['Sales_Diff_Pct'].abs() > 1.0]
if len(significant_diff) > 0:
    print(f"[!] {len(significant_diff)} dates with >1% difference:")
    for _, row in significant_diff.iterrows():
        print(f"   {row['ParsedDate'].strftime('%d/%m/%Y')}: {row['Sales_Diff_Pct']:.4f}%")
    print()

# Perfect matches
perfect_matches = matching[matching['Sales_Diff'].abs() < 0.01]
if len(perfect_matches) > 0:
    print(f"[SUCCESS] {len(perfect_matches)} dates with PERFECT match (<RM 0.01 diff):")
    for _, row in perfect_matches.head(5).iterrows():
        print(f"   {row['ParsedDate'].strftime('%d/%m/%Y')}: Xilnex RM {row['Xilnex_Sales']:,.2f} = Our RM {row['Our_Sales']:,.2f}")
    if len(perfect_matches) > 5:
        print(f"   ... and {len(perfect_matches) - 5} more")
    print()

print("="*80)
print("COMPARISON COMPLETE")
print("="*80)

# Save detailed comparison to Excel
output_file = f"Completed_Status_Comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    # Summary sheet
    summary_data = {
        'Metric': ['Sales Amount', 'Profit Amount'],
        'Xilnex Total': [xilnex_total_sales, xilnex_total_profit],
        'Our Total': [our_total_sales, our_total_profit],
        'Difference': [total_diff, profit_diff],
        'Difference %': [total_diff_pct, profit_diff_pct],
        'Accuracy %': [sales_accuracy, profit_accuracy]
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    # Detailed comparison sheet
    detailed = comparison[comparison['_merge'] == 'both'].copy()
    detailed = detailed[['ParsedDate', 'Xilnex_Sales', 'Our_Sales', 'Sales_Diff', 'Sales_Diff_Pct',
                         'Xilnex_Profit', 'Our_Profit', 'Profit_Diff', 'Profit_Diff_Pct']]
    detailed = detailed.sort_values('ParsedDate')
    detailed.to_excel(writer, sheet_name='Daily Comparison', index=False)

print(f"\n[OK] Detailed comparison saved to: {output_file}")

