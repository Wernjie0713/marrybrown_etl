"""
Intelligent Excel Comparison Tool for EOD Summary Detail Reports
Compares Portal vs Xilnex EOD Detail exports at cashier/sale type level
Aggregates both reports to Date + Store + Cashier + Sale Type for compatibility
Generates detailed variance analysis and discrepancy reports

Note: Aggregation excludes Card Type/Terminal ID due to structural differences:
- Portal: One row per Date+Store+Cashier+SaleType+CardType combination
- Xilnex: Multiple rows with subtotals and hierarchical grouping
- Solution: Aggregate both to Date+Store+Cashier+SaleType level for meaningful comparison
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# File paths
PORTAL_FILE = r"C:\Users\MIS INTERN\Downloads\EOD_Summary_Detail_2025-10-10_to_2025-10-16.xlsx"
XILNEX_FILE = r"C:\Users\MIS INTERN\Downloads\EOD Sales Summary Report (Detail)_22-10-2025 (Web).xlsx"

# Comparison tolerance
TOLERANCE_PERCENT = 0.5  # 0.5% variance acceptable
TOLERANCE_AMOUNT = 1.0   # RM 1 variance acceptable

def explore_excel_structure(file_path, file_type):
    """Explore and display Excel file structure."""
    print(f"\n{'='*80}")
    print(f"EXPLORING {file_type.upper()} FILE STRUCTURE")
    print(f"{'='*80}")
    print(f"File: {os.path.basename(file_path)}")
    
    try:
        # Get all sheet names
        xl_file = pd.ExcelFile(file_path)
        print(f"\nSheets found: {len(xl_file.sheet_names)}")
        for i, sheet_name in enumerate(xl_file.sheet_names, 1):
            print(f"  {i}. {sheet_name}")
        
        # Read first sheet
        first_sheet = xl_file.sheet_names[0]
        if file_type.lower() == 'xilnex':
            df = pd.read_excel(xl_file, sheet_name=first_sheet, header=13)
        else:
            df = pd.read_excel(xl_file, sheet_name=first_sheet)
        
        print(f"\nFirst Sheet: '{first_sheet}'")
        print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        
        print(f"\nColumn Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        print(f"\nFirst 5 rows preview:")
        print(df.head(5).to_string())
        
    except Exception as e:
        print(f"Error exploring file: {e}")

def normalize_portal_data(file_path):
    """Normalize Portal export data for comparison."""
    print("\n[Portal] Normalizing data structure...")
    
    # Read the Portal export
    df = pd.read_excel(file_path, sheet_name=0)
    
    # Standardize column names
    column_mapping = {
        'Date': 'Date',
        'Store': 'Store',
        'Terminal ID': 'Terminal ID',
        'Cashier Name': 'Cashier',
        'Sales Number': 'Sales Number',
        'Sales Type': 'Sale Type',
        'Items Count': 'Items Count',
        'Total Sales (RM)': 'Total Sales',
        'Sales Net Amount (RM)': 'Sales Net Amount',
        'Sales Net Amount ex. MGST Tax (RM)': 'Sales Net Amount ex Tax',
        'MGST Tax Amount (RM)': 'Tax Amount',
        'Cash Amount (RM)': 'Cash Amount',
        'Card Amount (RM)': 'Card Amount',
        'Voucher Amount (RM)': 'Voucher Amount',
        'E-Wallet Amount (RM)': 'E-Wallet Amount',
        'Other Amount (RM)': 'Other Amount',
        'Card Type': 'Card Type'
    }
    
    # Apply renaming
    normalized = df.rename(columns=column_mapping)
    
    # Convert date to standard format
    if 'Date' in normalized.columns:
        normalized['Date'] = pd.to_datetime(normalized['Date'], errors='coerce').dt.strftime('%d/%m/%Y')
    
    print(f"  Read {len(normalized)} rows (before aggregation)")
    
    # AGGREGATE by Date + Store + Cashier + Sale Type (exclude Card Type for compatibility)
    groupby_cols = ['Date', 'Store', 'Cashier', 'Sale Type']
    numeric_cols = ['Total Sales', 'Sales Net Amount', 'Sales Net Amount ex Tax', 'Tax Amount',
                    'Cash Amount', 'Card Amount', 'Voucher Amount', 'E-Wallet Amount', 'Other Amount']
    
    # Aggregate
    agg_dict = {col: 'sum' for col in numeric_cols if col in normalized.columns}
    normalized = normalized.groupby(groupby_cols, as_index=False).agg(agg_dict)
    
    # Convert to numeric and round to 2 decimal places
    for col in numeric_cols:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0).round(2)
    
    print(f"  Aggregated to {len(normalized)} rows (by Date + Store + Cashier + Sale Type)")
    print(f"  Columns: {list(normalized.columns)}")
    
    return normalized

def normalize_xilnex_data(file_path):
    """Normalize Xilnex export data for comparison."""
    print("\n[Xilnex] Normalizing data structure...")
    
    # Read data starting from header row 13
    normalized = pd.read_excel(file_path, sheet_name=0, header=13)
    print(f"  Read {len(normalized)} rows from row 13")
    
    # Remove rows where all values are NaN
    normalized = normalized.dropna(how='all')
    
    # Fill forward Date, Store, Cashier columns (Xilnex uses NaN for continuation)
    for col in ['Date', 'Store', 'Cashier']:
        if col in normalized.columns:
            normalized[col] = normalized[col].ffill()
            print(f"  Filled forward {col} values")
    
    # Remove "Total" rows (multiple patterns)
    before_count = len(normalized)
    if 'Cashier' in normalized.columns:
        normalized = normalized[~normalized['Cashier'].astype(str).str.contains('Total|Grand', case=False, na=False)]
    if 'Sales Type' in normalized.columns:
        normalized = normalized[~normalized['Sales Type'].astype(str).str.contains('Total', case=False, na=False)]
    if 'Card Type' in normalized.columns:
        # Remove rows where Card Type contains "Total" (subtotal rows)
        normalized = normalized[~normalized['Card Type'].astype(str).str.contains('Total', case=False, na=False)]
    removed_count = before_count - len(normalized)
    if removed_count > 0:
        print(f"  Removed {removed_count} total/summary rows")
    
    # Standardize column names
    column_mapping = {
        'Date': 'Date',
        'Store': 'Store',
        'Cashier': 'Cashier',
        'Computer Site ID': 'Terminal ID',
        'Sales Type': 'Sale Type',
        'Card Type': 'Card Type',
        'Total Sales': 'Total Sales',
        'Sales Net Amount': 'Sales Net Amount',
        'Sales Net Amount ex. MGST Tax': 'Sales Net Amount ex Tax',
        'Cash Amount': 'Cash Amount',
        'Other Amount': 'Other Amount',
        'Voucher Amount': 'Voucher Amount',
        'Card Amount': 'Card Amount',
        'E-Wallet Amount': 'E-Wallet Amount'
    }
    normalized = normalized.rename(columns=column_mapping)
    
    # Convert date to standard format
    if 'Date' in normalized.columns:
        try:
            normalized['Date'] = pd.to_datetime(normalized['Date'], errors='coerce').dt.strftime('%d/%m/%Y')
        except:
            pass
    
    print(f"  Cleaned to {len(normalized)} rows (before aggregation)")
    
    # AGGREGATE by Date + Store + Cashier + Sale Type (same as Portal)
    groupby_cols = ['Date', 'Store', 'Cashier', 'Sale Type']
    numeric_cols = ['Total Sales', 'Sales Net Amount', 'Sales Net Amount ex Tax',
                    'Cash Amount', 'Card Amount', 'Voucher Amount', 'E-Wallet Amount', 'Other Amount']
    
    # Aggregate
    agg_dict = {col: 'sum' for col in numeric_cols if col in normalized.columns}
    normalized = normalized.groupby(groupby_cols, as_index=False).agg(agg_dict)
    
    # Convert to numeric and round to 2 decimal places
    for col in numeric_cols:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0).round(2)
    
    print(f"  Aggregated to {len(normalized)} rows (by Date + Store + Cashier + Sale Type)")
    print(f"  Columns: {list(normalized.columns)}")
    
    return normalized

def compare_dataframes(portal_df, xilnex_df, key_columns):
    """Compare two dataframes and calculate variances."""
    print(f"\n{'='*80}")
    print("COMPARING DATA")
    print(f"{'='*80}")
    
    # Merge on key columns
    print(f"\nMerging on: {key_columns}")
    
    merged = pd.merge(
        portal_df,
        xilnex_df,
        on=key_columns,
        how='outer',
        suffixes=('_Portal', '_Xilnex'),
        indicator=True
    )
    
    print(f"  Portal rows: {len(portal_df)}")
    print(f"  Xilnex rows: {len(xilnex_df)}")
    print(f"  Matched rows: {len(merged[merged['_merge'] == 'both'])}")
    print(f"  Only in Portal: {len(merged[merged['_merge'] == 'left_only'])}")
    print(f"  Only in Xilnex: {len(merged[merged['_merge'] == 'right_only'])}")
    
    # Get numeric columns to compare
    numeric_cols_portal = portal_df.select_dtypes(include=[np.number]).columns
    numeric_cols_xilnex = xilnex_df.select_dtypes(include=[np.number]).columns
    
    # Find common numeric columns
    common_numeric = list(set(numeric_cols_portal) & set(numeric_cols_xilnex))
    
    # Exclude certain columns from comparison
    common_numeric = [col for col in common_numeric if col not in ['Terminal ID', 'Items Count', 'Sales Number']]
    
    print(f"\nNumeric columns to compare: {common_numeric}")
    
    return merged, common_numeric

def generate_comparison_report(merged_df, numeric_columns, output_path):
    """Generate Excel report with comparison results."""
    print(f"\n{'='*80}")
    print("GENERATING COMPARISON REPORT")
    print(f"{'='*80}")
    
    writer = pd.ExcelWriter(output_path, engine='openpyxl')
    
    # Sheet 1: Summary
    summary_data = []
    total_passed = 0
    total_failed = 0
    
    for col in numeric_columns:
        portal_col = f"{col}_Portal"
        xilnex_col = f"{col}_Xilnex"
        
        if portal_col in merged_df.columns and xilnex_col in merged_df.columns:
            # Calculate totals
            portal_total = merged_df[portal_col].sum()
            xilnex_total = merged_df[xilnex_col].sum()
            variance = portal_total - xilnex_total
            variance_pct = (variance / xilnex_total * 100) if xilnex_total != 0 else 0
            
            # Count passed/failed
            merged_df[f'{col}_Variance'] = merged_df[portal_col] - merged_df[xilnex_col]
            merged_df[f'{col}_Variance_Pct'] = (merged_df[f'{col}_Variance'] / merged_df[xilnex_col].abs() * 100).replace([np.inf, -np.inf], 0).fillna(0)
            merged_df[f'{col}_Pass'] = (
                (merged_df[f'{col}_Variance'].abs() <= TOLERANCE_AMOUNT) |
                (merged_df[f'{col}_Variance_Pct'].abs() <= TOLERANCE_PERCENT)
            )
            
            passed = merged_df[merged_df['_merge'] == 'both'][f'{col}_Pass'].sum()
            failed = len(merged_df[merged_df['_merge'] == 'both']) - passed
            pass_rate = (passed / len(merged_df[merged_df['_merge'] == 'both']) * 100) if len(merged_df[merged_df['_merge'] == 'both']) > 0 else 0
            
            total_passed += passed
            total_failed += failed
            
            summary_data.append({
                'Field': col,
                'Portal_Total': portal_total,
                'Xilnex_Total': xilnex_total,
                'Variance_RM': variance,
                'Variance_%': variance_pct,
                'Passed': passed,
                'Failed': failed,
                'Pass_Rate_%': pass_rate
            })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)
    print(f"  [1/4] Summary sheet created")
    
    # Sheet 2: Detailed Comparison
    # Prepare detailed data (aggregated level: no Terminal ID or Card Type)
    detail_cols = ['Date', 'Store', 'Cashier', 'Sale Type', '_merge']
    for col in numeric_columns:
        detail_cols.extend([f'{col}_Portal', f'{col}_Xilnex', f'{col}_Variance', f'{col}_Variance_Pct', f'{col}_Pass'])
    
    # Filter to only include existing columns
    detail_cols = [col for col in detail_cols if col in merged_df.columns]
    
    detail_df = merged_df[detail_cols].copy()
    detail_df.to_excel(writer, sheet_name='Detailed Comparison', index=False)
    print(f"  [2/4] Detailed Comparison sheet created ({len(detail_df)} rows)")
    
    # Sheet 3: Discrepancies Only
    discrepancy_mask = merged_df[[f'{col}_Pass' for col in numeric_columns if f'{col}_Pass' in merged_df.columns]].any(axis=1) == False
    discrepancies_df = merged_df[discrepancy_mask][detail_cols].copy()
    discrepancies_df.to_excel(writer, sheet_name='Discrepancies', index=False)
    print(f"  [3/4] Discrepancies sheet created ({len(discrepancies_df)} rows)")
    
    # Sheet 4: Unmatched Records
    unmatched_cols = ['Date', 'Store', 'Cashier', 'Sale Type', '_merge']
    unmatched_cols = [col for col in unmatched_cols if col in merged_df.columns]
    unmatched_df = merged_df[merged_df['_merge'] != 'both'][unmatched_cols].copy()
    unmatched_df.to_excel(writer, sheet_name='Unmatched Records', index=False)
    print(f"  [4/4] Unmatched Records sheet created ({len(unmatched_df)} rows)")
    
    writer.close()
    
    return summary_df, total_passed, total_failed

def main():
    """Main comparison function."""
    print("="*80)
    print("INTELLIGENT EOD SUMMARY DETAIL REPORT COMPARISON")
    print("="*80)
    print(f"\nPortal File: {os.path.basename(PORTAL_FILE)}")
    print(f"Xilnex File: {os.path.basename(XILNEX_FILE)}")
    print(f"\nTolerance Settings:")
    print(f"  Percentage: {TOLERANCE_PERCENT}%")
    print(f"  Amount: RM {TOLERANCE_AMOUNT}")
    
    # Explore file structures
    explore_excel_structure(PORTAL_FILE, "Portal")
    explore_excel_structure(XILNEX_FILE, "Xilnex")
    
    # Normalize data
    portal_df = normalize_portal_data(PORTAL_FILE)
    xilnex_df = normalize_xilnex_data(XILNEX_FILE)
    
    if portal_df.empty or xilnex_df.empty:
        print("\n[ERROR] Failed to normalize data. Exiting...")
        return
    
    # Find common columns
    common_cols = list(set(portal_df.columns) & set(xilnex_df.columns))
    print(f"\nCommon columns: {common_cols}")
    
    # Define key columns for matching (aggregated level, no Card Type)
    key_columns = ['Date', 'Store', 'Cashier', 'Sale Type']
    print(f"\nUsing key columns for matching: {key_columns}")
    
    # Compare dataframes
    merged_df, numeric_columns = compare_dataframes(portal_df, xilnex_df, key_columns)
    
    # Generate report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"EOD_Detail_Comparison_Report_{timestamp}.xlsx"
    
    print(f"\n{'='*80}")
    print("COMPARISON SUMMARY")
    print(f"{'='*80}")
    
    summary_df, total_passed, total_failed = generate_comparison_report(
        merged_df, 
        numeric_columns, 
        output_file
    )
    
    # Display summary
    print(summary_df.to_string(index=False))
    
    # Final verdict
    print(f"\n{'='*80}")
    print("FINAL VERDICT")
    print(f"{'='*80}")
    
    total_comparisons = total_passed + total_failed
    pass_rate = (total_passed / total_comparisons * 100) if total_comparisons > 0 else 0
    
    print(f"Total Comparisons: {total_comparisons}")
    print(f"Passed: {total_passed}")
    print(f"Failed: {total_failed}")
    print(f"Pass Rate: {pass_rate:.2f}%")
    
    if pass_rate >= 95:
        print(f"\n[SUCCESS] {pass_rate:.2f}% pass rate - Excellent data quality")
    elif pass_rate >= 90:
        print(f"\n[GOOD] {pass_rate:.2f}% pass rate - Good data quality")
    else:
        print(f"\n[CRITICAL] {pass_rate:.2f}% pass rate - Major discrepancies found")
    
    print(f"\nOpen '{output_file}' for detailed analysis")
    print(f"\nNOTE: Comparison at Date + Store + Cashier + Sale Type level")
    print(f"      (Card Type excluded due to structural differences)")
    print("="*80)

if __name__ == "__main__":
    main()

