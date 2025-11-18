"""
Intelligent Excel Comparison Tool for Detailed Daily Sales Reports
Compares Portal vs Xilnex exports with sale type breakdown
Generates detailed variance analysis and discrepancy reports
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# File paths
PORTAL_FILE = r"C:\Users\MIS INTERN\Downloads\Detailed_Daily_Sales_Report_2025-10-10_to_2025-10-16 (1).xlsx"
XILNEX_FILE = r"C:\Users\MIS INTERN\Downloads\Daily Sales_21-10-2025 (Web).xlsx"

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
        
        # Read first sheet
        first_sheet = xl_file.sheet_names[0]
        df = pd.read_excel(file_path, sheet_name=first_sheet)
        
        print(f"\nFirst Sheet: '{first_sheet}'")
        print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        print(f"\nColumn Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        print(f"\nFirst 3 rows preview:")
        print(df.head(3).to_string(index=False))
        
        return xl_file, df
    except Exception as e:
        print(f"ERROR exploring {file_path}: {e}")
        return None, None

def normalize_portal_data(df):
    """Normalize Portal export data for comparison."""
    print("\n[Portal] Normalizing data structure...")
    
    normalized = df.copy()
    
    # Rename columns to standard names if needed
    column_mapping = {
        'date': 'Date',
        'store_name': 'Store',
        'sale_type': 'Sale Type',
        'sales_amount': 'Sales Amount (RM)',
        'sales_amount_ex_tax': 'Sales Amount ex Tax (RM)',
        'profit_amount': 'Profit Amount (RM)'
    }
    
    for old_col, new_col in column_mapping.items():
        if old_col in normalized.columns:
            normalized = normalized.rename(columns={old_col: new_col})
    
    # Convert date to standard format
    if 'Date' in normalized.columns:
        normalized['Date'] = pd.to_datetime(normalized['Date'], errors='coerce').dt.strftime('%d/%m/%Y')
        normalized = normalized[normalized['Date'].notna()]
    
    # Round numeric columns to 2 decimal places
    numeric_cols = normalized.select_dtypes(include=[np.number]).columns
    normalized[numeric_cols] = normalized[numeric_cols].round(2)
    
    # Clean up sale type values (standardize capitalization)
    if 'Sale Type' in normalized.columns:
        normalized['Sale Type'] = normalized['Sale Type'].str.strip()
    
    print(f"  Normalized {len(normalized)} rows")
    print(f"  Columns: {list(normalized.columns)}")
    
    return normalized

def normalize_xilnex_data(file_path):
    """Normalize Xilnex export data for comparison."""
    print("\n[Xilnex] Normalizing data structure...")
    
    # Read raw data without header to find the actual data table
    df_raw = pd.read_excel(file_path, sheet_name=0, header=None)
    
    # Find the header row (contains "Store", "Sales Type", "Sales Amount")
    header_row_idx = None
    for i, row in df_raw.iterrows():
        row_str = ' '.join([str(x) for x in row.values if pd.notna(x)])
        # Look for key column names in the row
        if 'Store' in row_str and ('Sales Type' in row_str or 'Type' in row_str) and 'Sales Amount' in row_str:
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
    
    # Remove rows with NaN dates
    if 'Date' in normalized.columns:
        before_count = len(normalized)
        normalized = normalized[normalized['Date'].notna()]
        removed_count = before_count - len(normalized)
        print(f"  Removed {removed_count} rows with missing dates")
    
    # Standardize column names
    column_mapping = {
        'Sales Type': 'Sale Type',
        'Type': 'Sale Type',
        'Sales Amount': 'Sales Amount (RM)',
        'Sales Amount ex. MGST Tax': 'Sales Amount ex Tax (RM)',
        'Sales Net Amount ex. MGST Tax': 'Sales Amount ex Tax (RM)',
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
    
    # Clean up sale type values
    if 'Sale Type' in normalized.columns:
        normalized['Sale Type'] = normalized['Sale Type'].str.strip()
    
    print(f"  Final normalized data: {len(normalized)} rows")
    print(f"  Columns: {list(normalized.columns)}")
    
    return normalized

def compare_dataframes(portal_df, xilnex_df, key_columns, numeric_columns, percentage_tolerance, amount_tolerance):
    """Compare two dataframes and calculate variances."""
    print(f"\n{'='*80}")
    print("COMPARING DATA")
    print(f"{'='*80}")
    
    # Merge dataframes on key columns
    merged_df = pd.merge(portal_df, xilnex_df, on=key_columns, how='outer', suffixes=('_Portal', '_Xilnex'), indicator=True)
    
    print(f"\nMerging on: {key_columns}")
    print(f"  Portal rows: {len(portal_df)}")
    print(f"  Xilnex rows: {len(xilnex_df)}")
    
    matched_rows = merged_df[merged_df['_merge'] == 'both']
    only_portal = merged_df[merged_df['_merge'] == 'left_only']
    only_xilnex = merged_df[merged_df['_merge'] == 'right_only']
    
    print(f"  Matched rows: {len(matched_rows)}")
    print(f"  Only in Portal: {len(only_portal)}")
    print(f"  Only in Xilnex: {len(only_xilnex)}")
    
    comparison_results = []
    summary_data = []
    
    for col in numeric_columns:
        portal_col = col + '_Portal'
        xilnex_col = col + '_Xilnex'
        
        # Ensure columns are numeric, coerce errors to NaN
        merged_df[portal_col] = pd.to_numeric(merged_df[portal_col], errors='coerce').fillna(0)
        merged_df[xilnex_col] = pd.to_numeric(merged_df[xilnex_col], errors='coerce').fillna(0)
        
        # Calculate variance
        merged_df[f'{col}_Variance_RM'] = merged_df[portal_col] - merged_df[xilnex_col]
        merged_df[f'{col}_Variance_%'] = np.where(
            merged_df[xilnex_col] != 0,
            (merged_df[f'{col}_Variance_RM'] / merged_df[xilnex_col]) * 100,
            np.where(merged_df[portal_col] != 0, 100, 0)
        )
        
        # Determine pass/fail for each row
        merged_df[f'{col}_Passed'] = (
            (abs(merged_df[f'{col}_Variance_%']) <= percentage_tolerance) |
            (abs(merged_df[f'{col}_Variance_RM']) <= amount_tolerance)
        )
        
        total_portal = merged_df[portal_col].sum()
        total_xilnex = merged_df[xilnex_col].sum()
        total_variance_rm = total_portal - total_xilnex
        total_variance_percent = (total_variance_rm / total_xilnex) * 100 if total_xilnex != 0 else (100 if total_portal != 0 else 0)
        
        passed_rows = merged_df[f'{col}_Passed'].sum()
        failed_rows = len(merged_df) - passed_rows
        pass_rate = (passed_rows / len(merged_df)) * 100 if len(merged_df) > 0 else 0
        
        summary_data.append({
            'Field': col,
            'Portal_Total': total_portal,
            'Xilnex_Total': total_xilnex,
            'Variance_RM': total_variance_rm,
            'Variance_%': total_variance_percent,
            'Passed': passed_rows,
            'Failed': failed_rows,
            'Pass_Rate_%': pass_rate
        })
    
    return merged_df, pd.DataFrame(summary_data)

def generate_comparison_report(merged_df, summary_df, output_dir, report_type):
    """Generates an Excel report with comparison results."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f"{report_type}_Comparison_Report_{timestamp}.xlsx")
    
    print(f"\n{'='*80}")
    print("GENERATING COMPARISON REPORT")
    print(f"{'='*80}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary Sheet
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        print("  [1/5] Summary sheet created")
        
        # Detailed Comparison Sheet
        merged_df.to_excel(writer, sheet_name='Detailed Comparison', index=False)
        print(f"  [2/5] Detailed Comparison sheet created ({len(merged_df)} rows)")
        
        # Discrepancies Sheet
        numeric_cols = [col for col in merged_df.columns if col.endswith('_Passed')]
        if numeric_cols:
            discrepancies_df = merged_df[~merged_df[numeric_cols].all(axis=1)]
            if not discrepancies_df.empty:
                discrepancies_df.to_excel(writer, sheet_name='Discrepancies', index=False)
                print(f"  [3/5] Discrepancies sheet created ({len(discrepancies_df)} rows)")
            else:
                pd.DataFrame({"Message": ["No discrepancies found based on set tolerance."]}).to_excel(writer, sheet_name='Discrepancies', index=False)
                print("  [3/5] No discrepancies found - created placeholder")
        
        # Only in Portal/Xilnex
        only_in_portal = merged_df[merged_df['_merge'] == 'left_only']
        only_in_xilnex = merged_df[merged_df['_merge'] == 'right_only']
        
        if not only_in_portal.empty:
            only_in_portal.to_excel(writer, sheet_name='Only in Portal', index=False)
            print(f"  [4/5] 'Only in Portal' sheet created ({len(only_in_portal)} rows)")
        if not only_in_xilnex.empty:
            only_in_xilnex.to_excel(writer, sheet_name='Only in Xilnex', index=False)
            print(f"  [5/5] 'Only in Xilnex' sheet created ({len(only_in_xilnex)} rows)")
        if only_in_portal.empty and only_in_xilnex.empty:
            pd.DataFrame({"Message": ["All records matched or no unique records found in either source."]}).to_excel(writer, sheet_name='Only Unique Records', index=False)
            print("  [4/5] All records matched - created placeholder")
    
    print(f"\n[SUCCESS] Report saved to: {output_file}")
    return output_file

def main():
    print(f"{'='*80}")
    print("INTELLIGENT DETAILED DAILY SALES REPORT COMPARISON")
    print(f"{'='*80}\n")
    
    print(f"Portal File: {os.path.basename(PORTAL_FILE)}")
    print(f"Xilnex File: {os.path.basename(XILNEX_FILE)}\n")
    print(f"Tolerance Settings:")
    print(f"  Percentage: {TOLERANCE_PERCENT}%")
    print(f"  Amount: RM {TOLERANCE_AMOUNT}\n")
    
    # Step 1: Explore file structures
    portal_xl, portal_raw = explore_excel_structure(PORTAL_FILE, "Portal")
    if portal_raw is None:
        return
    
    xilnex_xl, xilnex_raw = explore_excel_structure(XILNEX_FILE, "Xilnex")
    if xilnex_raw is None:
        return
    
    # Step 2: Normalize both datasets
    portal_normalized = normalize_portal_data(portal_raw)
    xilnex_normalized = normalize_xilnex_data(XILNEX_FILE)
    
    # Step 3: Identify common columns
    common_cols = set(portal_normalized.columns) & set(xilnex_normalized.columns)
    print(f"\nCommon columns: {list(common_cols)}")
    
    # Check if Date column exists in both (if not, we need to aggregate Portal data)
    if 'Date' not in xilnex_normalized.columns and 'Date' in portal_normalized.columns:
        print(f"\n[NOTE] Xilnex data is aggregated across date range")
        print(f"  Xilnex structure: Store + Sale Type (totals)")
        print(f"  Portal structure: Date + Store + Sale Type (daily breakdown)")
        print(f"  Solution: Aggregating Portal data to match Xilnex structure...")
        
        # Aggregate Portal data by Store and Sale Type
        numeric_cols = portal_normalized.select_dtypes(include=[np.number]).columns.tolist()
        group_cols = ['Store', 'Sale Type']
        
        portal_aggregated = portal_normalized.groupby(group_cols, as_index=False)[numeric_cols].sum()
        print(f"  Portal aggregated: {len(portal_aggregated)} rows (from {len(portal_normalized)} daily rows)")
        
        portal_normalized = portal_aggregated
    
    # Key columns for detailed report
    KEY_COLUMNS = ['Store', 'Sale Type']
    effective_key_columns = [col for col in KEY_COLUMNS if col in common_cols]
    
    if not effective_key_columns or len(effective_key_columns) < 2:
        print(f"[ERROR] Missing required key columns!")
        print(f"Required: {KEY_COLUMNS}")
        print(f"Found: {effective_key_columns}")
        print(f"Portal columns: {list(portal_normalized.columns)}")
        print(f"Xilnex columns: {list(xilnex_normalized.columns)}")
        return
    
    print(f"\nUsing key columns for matching: {effective_key_columns}")
    
    # Numeric columns to compare
    NUMERIC_COLUMNS = [
        'Sales Amount (RM)',
        'Sales Amount ex Tax (RM)',
        'Profit Amount (RM)'
    ]
    effective_numeric_columns = [col for col in NUMERIC_COLUMNS if col in common_cols]
    print(f"Comparing numeric columns: {effective_numeric_columns}")
    
    # Step 4: Compare dataframes
    merged_df, summary_df = compare_dataframes(
        portal_normalized,
        xilnex_normalized,
        effective_key_columns,
        effective_numeric_columns,
        TOLERANCE_PERCENT,
        TOLERANCE_AMOUNT
    )
    
    # Step 5: Generate report
    output_dir = os.path.dirname(PORTAL_FILE)
    output_report_path = generate_comparison_report(merged_df, summary_df, output_dir, "Detailed_Daily_Sales")
    
    print(f"\n{'='*80}")
    print("COMPARISON SUMMARY")
    print(f"{'='*80}")
    print(summary_df.to_string(index=False))
    
    print(f"\n{'='*80}")
    print("FINAL VERDICT")
    print(f"{'='*80}")
    
    total_comparisons = summary_df['Passed'].sum() + summary_df['Failed'].sum()
    total_passed = summary_df['Passed'].sum()
    total_failed = summary_df['Failed'].sum()
    overall_pass_rate = (total_passed / total_comparisons) * 100 if total_comparisons > 0 else 0
    
    print(f"Total Comparisons: {total_comparisons}")
    print(f"Passed: {total_passed}")
    print(f"Failed: {total_failed}")
    print(f"Pass Rate: {overall_pass_rate:.2f}%")
    
    if overall_pass_rate < 100:
        print(f"\n[CRITICAL] {overall_pass_rate:.2f}% pass rate - Discrepancies found")
    else:
        print("\n[SUCCESS] All comparisons passed within tolerance!")
    
    print(f"\nOpen '{os.path.basename(output_report_path)}' for detailed analysis")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()

