"""
Intelligent Excel Comparison Tool for Daily Sales Reports
Compares Portal exports vs Xilnex exports with different structures
Generates detailed variance analysis and discrepancy reports
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# File paths
PORTAL_FILE = r"C:\Users\MIS INTERN\Downloads\Daily_Sales_Report_2025-10-10_to_2025-10-16 (1).xlsx"
XILNEX_FILE = r"C:\Users\MIS INTERN\Downloads\Daily Sales_22-10-2025 (Web).xlsx"

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
        for i, sheet in enumerate(xl_file.sheet_names, 1):
            print(f"  {i}. {sheet}")
        
        # Read first sheet and show structure
        df = pd.read_excel(file_path, sheet_name=0)
        print(f"\nFirst Sheet: '{xl_file.sheet_names[0]}'")
        print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        print(f"\nColumn Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        print(f"\nFirst 3 rows preview:")
        print(df.head(3).to_string())
        
        return xl_file, df
        
    except Exception as e:
        print(f"ERROR reading file: {e}")
        return None, None

def normalize_portal_data(df):
    """Normalize portal export data for comparison."""
    print("\n[Portal] Normalizing data structure...")
    
    # Expected columns from portal (adjust based on actual structure)
    # The portal typically has: Date, Store, Sale Type, Sales Amount, etc.
    
    normalized = df.copy()
    
    # Rename columns to standard names (if needed)
    column_mapping = {
        'date': 'Date',
        'store_name': 'Store',
        'Store': 'Store',  # Keep as is
        'Date': 'Date',  # Keep as is
        'Sales Amount (RM)': 'Sales Amount (RM)',
        'Profit Amount (RM)': 'Profit Amount (RM)'
    }
    
    # Apply renaming if columns exist
    for old_col, new_col in column_mapping.items():
        if old_col in normalized.columns:
            normalized = normalized.rename(columns={old_col: new_col})
    
    # Convert date to standard format
    if 'Date' in normalized.columns:
        normalized['Date'] = pd.to_datetime(normalized['Date']).dt.strftime('%d/%m/%Y')
    
    # Round numeric columns to 2 decimal places
    numeric_cols = normalized.select_dtypes(include=[np.number]).columns
    normalized[numeric_cols] = normalized[numeric_cols].round(2)
    
    print(f"  Normalized {len(normalized)} rows")
    print(f"  Columns: {list(normalized.columns)}")
    
    return normalized

def normalize_xilnex_data(file_path):
    """Normalize Xilnex export data for comparison."""
    print("\n[Xilnex] Normalizing data structure...")
    
    # Read raw data without header to find the actual data table
    df_raw = pd.read_excel(file_path, sheet_name=0, header=None)
    
    # Find the header row (contains "Store", "Date", "Sales Amount")
    header_row_idx = None
    for i, row in df_raw.iterrows():
        row_str = ' '.join([str(x) for x in row.values if pd.notna(x)])
        if 'Store' in row_str and 'Date' in row_str and 'Sales Amount' in row_str:
            header_row_idx = i
            print(f"  Found header row at index {i}")
            break
    
    if header_row_idx is None:
        print("  ERROR: Could not find header row!")
        return pd.DataFrame()
    
    # Read data starting from header row
    normalized = pd.read_excel(file_path, sheet_name=0, header=header_row_idx)
    print(f"  Read {len(normalized)} rows from row {header_row_idx}")
    
    # Remove rows where all values are NaN
    normalized = normalized.dropna(how='all')
    
    # Fill forward Date column (Xilnex uses NaN for continuation within date groups)
    if 'Date' in normalized.columns:
        normalized['Date'] = normalized['Date'].ffill()
        print(f"  Filled forward Date values")
    
    # Fill forward Store column (Xilnex uses NaN for continuation)
    if 'Store' in normalized.columns:
        normalized['Store'] = normalized['Store'].ffill()
        print(f"  Filled forward Store names")
    
    # Remove "Total" rows (store totals and grand totals)
    if 'Store' in normalized.columns:
        before_count = len(normalized)
        normalized = normalized[~normalized['Store'].astype(str).str.contains('Total|Grand', case=False, na=False)]
        removed_count = before_count - len(normalized)
        print(f"  Removed {removed_count} total rows")
    
    # Remove rows with NaN dates AFTER forward-fill (these are actual missing data)
    if 'Date' in normalized.columns:
        before_count = len(normalized)
        normalized = normalized[normalized['Date'].notna()]
        removed_count = before_count - len(normalized)
        if removed_count > 0:
            print(f"  Removed {removed_count} rows with missing dates")
    
    # Standardize column names
    column_mapping = {
        'Sales Amount': 'Sales Amount (RM)',
        'Profit Amount': 'Profit Amount (RM)'
    }
    normalized = normalized.rename(columns=column_mapping)
    
    # Convert date to standard format
    if 'Date' in normalized.columns:
        try:
            normalized['Date'] = pd.to_datetime(normalized['Date'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        except:
            pass
    
    # Round numeric columns to 2 decimal places
    numeric_cols = normalized.select_dtypes(include=[np.number]).columns
    normalized[numeric_cols] = normalized[numeric_cols].round(2)
    
    print(f"  Final normalized data: {len(normalized)} rows")
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
    
    # Find numeric columns that exist in both
    numeric_cols = []
    for col in portal_df.columns:
        if col not in key_columns and pd.api.types.is_numeric_dtype(portal_df[col]):
            if col in xilnex_df.columns:
                numeric_cols.append(col)
    
    print(f"\nNumeric columns to compare: {numeric_cols}")
    
    # Calculate variances for matched rows
    comparison_results = []
    
    for col in numeric_cols:
        portal_col = f"{col}_Portal"
        xilnex_col = f"{col}_Xilnex"
        
        if portal_col in merged.columns and xilnex_col in merged.columns:
            # Calculate variance
            merged[f'{col}_Variance_RM'] = merged[portal_col] - merged[xilnex_col]
            merged[f'{col}_Variance_%'] = np.where(
                merged[xilnex_col] != 0,
                (merged[f'{col}_Variance_RM'] / merged[xilnex_col] * 100),
                0
            )
            
            # Flag discrepancies
            merged[f'{col}_Status'] = 'PASS'
            merged.loc[
                (abs(merged[f'{col}_Variance_%']) > TOLERANCE_PERCENT) & 
                (abs(merged[f'{col}_Variance_RM']) > TOLERANCE_AMOUNT),
                f'{col}_Status'
            ] = 'FAIL'
            
            # Summarize results for this column
            matched = merged[merged['_merge'] == 'both']
            total_portal = matched[portal_col].sum()
            total_xilnex = matched[xilnex_col].sum()
            total_variance = total_portal - total_xilnex
            total_variance_pct = (total_variance / total_xilnex * 100) if total_xilnex != 0 else 0
            
            passed = len(matched[matched[f'{col}_Status'] == 'PASS'])
            failed = len(matched[matched[f'{col}_Status'] == 'FAIL'])
            
            comparison_results.append({
                'Field': col,
                'Portal_Total': total_portal,
                'Xilnex_Total': total_xilnex,
                'Variance_RM': total_variance,
                'Variance_%': total_variance_pct,
                'Passed': passed,
                'Failed': failed,
                'Pass_Rate_%': (passed / len(matched) * 100) if len(matched) > 0 else 0
            })
    
    return merged, pd.DataFrame(comparison_results)

def generate_comparison_report(merged_df, summary_df, output_file):
    """Generate detailed Excel comparison report."""
    print(f"\n{'='*80}")
    print("GENERATING COMPARISON REPORT")
    print(f"{'='*80}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Sheet 1: Executive Summary
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        print(f"  [1/4] Summary sheet created")
        
        # Sheet 2: Detailed Comparison (all rows)
        merged_df.to_excel(writer, sheet_name='Detailed Comparison', index=False)
        print(f"  [2/4] Detailed Comparison sheet created ({len(merged_df)} rows)")
        
        # Sheet 3: Discrepancies Only
        discrepancies = merged_df[
            merged_df.filter(like='_Status').eq('FAIL').any(axis=1)
        ]
        if len(discrepancies) > 0:
            discrepancies.to_excel(writer, sheet_name='Discrepancies', index=False)
            print(f"  [3/4] Discrepancies sheet created ({len(discrepancies)} rows)")
        else:
            pd.DataFrame({'Message': ['No discrepancies found!']}).to_excel(
                writer, sheet_name='Discrepancies', index=False
            )
            print(f"  [3/4] No discrepancies found - created placeholder")
        
        # Sheet 4: Unmatched Records
        unmatched = merged_df[merged_df['_merge'] != 'both']
        if len(unmatched) > 0:
            unmatched.to_excel(writer, sheet_name='Unmatched Records', index=False)
            print(f"  [4/4] Unmatched Records sheet created ({len(unmatched)} rows)")
        else:
            pd.DataFrame({'Message': ['All records matched!']}).to_excel(
                writer, sheet_name='Unmatched Records', index=False
            )
            print(f"  [4/4] All records matched - created placeholder")
    
    print(f"\n[SUCCESS] Report saved to: {output_file}")

def main():
    print("="*80)
    print("INTELLIGENT DAILY SALES REPORT COMPARISON")
    print("="*80)
    print(f"\nPortal File: {os.path.basename(PORTAL_FILE)}")
    print(f"Xilnex File: {os.path.basename(XILNEX_FILE)}")
    print(f"\nTolerance Settings:")
    print(f"  Percentage: {TOLERANCE_PERCENT}%")
    print(f"  Amount: RM {TOLERANCE_AMOUNT}")
    
    # Step 1: Explore both files
    portal_xl, portal_raw = explore_excel_structure(PORTAL_FILE, "Portal")
    if portal_raw is None:
        return
    
    xilnex_xl, xilnex_raw = explore_excel_structure(XILNEX_FILE, "Xilnex")
    if xilnex_raw is None:
        return
    
    # Step 2: Normalize both datasets
    portal_normalized = normalize_portal_data(portal_raw)
    xilnex_normalized = normalize_xilnex_data(XILNEX_FILE)
    
    # Step 3: Identify common key columns
    common_cols = set(portal_normalized.columns) & set(xilnex_normalized.columns)
    print(f"\nCommon columns: {list(common_cols)}")
    
    # Define key columns for matching (adjust based on actual data)
    key_columns = []
    for col in ['Date', 'Store', 'Sale Type']:
        if col in common_cols:
            key_columns.append(col)
    
    if not key_columns:
        print("\n[ERROR] No common key columns found for matching!")
        print("Portal columns:", list(portal_normalized.columns))
        print("Xilnex columns:", list(xilnex_normalized.columns))
        return
    
    print(f"\nUsing key columns for matching: {key_columns}")
    
    # Step 4: Compare data
    merged_df, summary_df = compare_dataframes(
        portal_normalized,
        xilnex_normalized,
        key_columns
    )
    
    # Step 5: Display summary
    print(f"\n{'='*80}")
    print("COMPARISON SUMMARY")
    print(f"{'='*80}")
    print(summary_df.to_string(index=False))
    
    # Step 6: Generate report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"Daily_Sales_Comparison_Report_{timestamp}.xlsx"
    generate_comparison_report(merged_df, summary_df, output_file)
    
    # Step 7: Final verdict
    print(f"\n{'='*80}")
    print("FINAL VERDICT")
    print(f"{'='*80}")
    
    total_failed = summary_df['Failed'].sum()
    total_tests = summary_df['Passed'].sum() + summary_df['Failed'].sum()
    pass_rate = (summary_df['Passed'].sum() / total_tests * 100) if total_tests > 0 else 0
    
    print(f"Total Comparisons: {total_tests:,}")
    print(f"Passed: {summary_df['Passed'].sum():,}")
    print(f"Failed: {total_failed:,}")
    print(f"Pass Rate: {pass_rate:.2f}%")
    
    if total_failed == 0:
        print("\n[SUCCESS] All comparisons PASSED! Data matches within tolerance.")
    elif pass_rate >= 95:
        print(f"\n[GOOD] {pass_rate:.2f}% pass rate - Minor discrepancies found")
    elif pass_rate >= 80:
        print(f"\n[WARNING] {pass_rate:.2f}% pass rate - Several discrepancies found")
    else:
        print(f"\n[CRITICAL] {pass_rate:.2f}% pass rate - Major discrepancies found")
    
    print(f"\nOpen '{output_file}' for detailed analysis")
    print("="*80)

if __name__ == "__main__":
    main()

