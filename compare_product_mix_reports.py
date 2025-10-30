"""
Product Mix Report Comparison Script
Compares our portal's Product Mix report against Xilnex portal's report
to identify discrepancies and data quality issues.

Date: October 27, 2025 (Week 6)
Purpose: Validate ETL accuracy for Product Mix report
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from tabulate import tabulate

# File paths
OUR_PORTAL_FILE = r"C:\Users\MIS INTERN\Downloads\product-mix-2025-09-01_to_2025-09-30.xlsx"
XILNEX_FILE = r"C:\Users\MIS INTERN\Downloads\Product Mix Report_27-10-2025 (Web).xlsx"

def load_our_portal_data():
    """Load data from our portal's Excel export"""
    print("\n" + "="*80)
    print("STEP 1: LOADING OUR PORTAL DATA")
    print("="*80)
    
    df = pd.read_excel(OUR_PORTAL_FILE)
    print(f"\n[OK] Loaded our portal data: {len(df):,} rows")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nFirst 3 rows:")
    print(df.head(3).to_string())
    
    return df

def load_xilnex_data():
    """Load data from Xilnex portal's Excel export (grouped format)"""
    print("\n" + "="*80)
    print("STEP 2: LOADING XILNEX PORTAL DATA")
    print("="*80)
    
    # Xilnex exports in grouped format with subtotals
    # We need to find the header row and filter out subtotal rows
    
    # First, load raw to inspect structure
    df_raw = pd.read_excel(XILNEX_FILE)
    print(f"\n[INFO] Raw file has {len(df_raw)} rows total")
    
    # Find the actual header row - look for a row with "Store" or "Item Name" pattern
    header_row = None
    for idx, row in df_raw.iterrows():
        # Check if this row has multiple column headers
        row_values = [str(val).strip().upper() for val in row if pd.notna(val)]
        
        # Look for typical Product Mix Report column names
        if any(keyword in ' '.join(row_values) for keyword in ['STORE', 'ITEM NAME', 'SALES TYPE', 'CATEGORY']):
            # Check if we have multiple headers (at least 5)
            if len([v for v in row_values if v and v != 'NAN']) >= 5:
                header_row = idx
                print(f"\n[OK] Found header row at index: {header_row}")
                print(f"    Headers: {[str(v) for v in row[:9].tolist()]}")
                break
    
    if header_row is None:
        print("\n[ERROR] Could not find header row!")
        print("Please manually inspect the Excel file.")
        return df_raw
    
    # Load again with proper header
    # Use header parameter to specify which row contains column names
    df = pd.read_excel(XILNEX_FILE, header=header_row)
    
    print(f"\n[INFO] Loaded {len(df)} rows from Xilnex (before cleaning)")
    print(f"\nColumns (raw): {list(df.columns)}")
    
    # Check if first row contains the actual column names (pandas issue with merged cells)
    if 'Unnamed' in str(df.columns[0]):
        print("\n[INFO] Column names not parsed correctly. Checking first data row...")
        first_row_values = df.iloc[0].tolist()
        print(f"First row values: {first_row_values}")
        
        # If first row has actual column names, use them
        if any('Store' in str(val) or 'Item' in str(val) for val in first_row_values):
            print("[OK] Using first data row as column names")
            df.columns = first_row_values
            df = df.iloc[1:]  # Remove the header row from data
            df = df.reset_index(drop=True)
            print(f"[OK] Corrected columns: {list(df.columns)}")
    
    print(f"\nColumns detected: {list(df.columns)}")
    
    # Clean the data:
    # 1. Remove rows that are all NaN
    df = df.dropna(how='all')
    
    # 2. Identify and remove subtotal/total rows
    # Subtotal rows typically have text like "Total", "Grand Total" in Item Name column
    # or have a category name without an item name
    item_col_idx = None
    for idx, col in enumerate(df.columns):
        if 'ITEM' in str(col).upper() or 'PRODUCT' in str(col).upper():
            item_col_idx = idx
            break
    
    if item_col_idx is not None:
        item_col = df.columns[item_col_idx]
        print(f"\n[INFO] Item column identified: '{item_col}'")
        
        # Filter out rows where Item Name contains "Total" or is empty
        before_count = len(df)
        df = df[df[item_col].notna()]  # Remove NaN items
        df = df[~df[item_col].astype(str).str.contains('Total', case=False, na=False)]
        df = df[~df[item_col].astype(str).str.contains('Grand Total', case=False, na=False)]
        after_count = len(df)
        
        print(f"[OK] Filtered out {before_count - after_count} subtotal/total rows")
        print(f"[OK] Remaining data rows: {after_count}")
    
    print(f"\nFirst 5 actual product rows:")
    print(df.head(5).to_string())
    
    return df

def normalize_column_names(df_our, df_xilnex):
    """
    Normalize column names between both reports for comparison.
    Map Xilnex column names to our portal's column names.
    """
    print("\n" + "="*80)
    print("STEP 3: NORMALIZING COLUMN NAMES")
    print("="*80)
    
    # First, let's see what columns we have
    print("\nOur portal columns:")
    for i, col in enumerate(df_our.columns, 1):
        print(f"  {i}. {col}")
    
    print("\nXilnex columns:")
    for i, col in enumerate(df_xilnex.columns, 1):
        print(f"  {i}. {col}")
    
    # Try to create automatic mapping based on common patterns
    column_mapping = {}
    
    xilnex_cols_lower = [str(col).lower().strip() for col in df_xilnex.columns]
    
    # Map each of our columns to Xilnex columns
    # Based on Xilnex screenshot: Store, Sales Type, Category, Item Name, Total Cost Amount, 
    # Unit Sold Price (Menu Item), Qty, Net Sold Price ex. Tax, Net Sold Price
    our_to_xilnex = {
        'Product Name': ['item name', 'product name', 'product', 'item'],
        'Category': ['category', 'product category', 'item category'],
        'Store': ['store', 'location', 'outlet'],
        'Sale Type': ['sales type', 'sale type', 'order type', 'type'],
        'Quantity Sold': ['qty', 'quantity', 'qty sold', 'quantity sold'],
        'Unit Sold Price (RM)': ['unit sold price', 'menu item', 'unit price', 'price'],
        'Net Sold Price ex. Tax (RM)': ['net sold price ex', 'net sold price ex. tax', 'amount ex tax'],
        'Net Sold Price (RM)': ['net sold price', 'total amount', 'amount'],
        'Total Cost (RM)': ['total cost amount', 'total cost', 'cost amount', 'cost'],
        'Profit (RM)': ['profit', 'net profit', 'gross profit'],
        'Profit Margin (%)': ['margin', 'profit margin', 'margin %', 'profit %']
    }
    
    for our_col, possible_xilnex_names in our_to_xilnex.items():
        for possible_name in possible_xilnex_names:
            for idx, xilnex_col in enumerate(xilnex_cols_lower):
                if possible_name in xilnex_col:
                    actual_xilnex_col = df_xilnex.columns[idx]
                    column_mapping[actual_xilnex_col] = our_col
                    print(f"  Mapped: '{actual_xilnex_col}' -> '{our_col}'")
                    break
            if our_col in [v for v in column_mapping.values()]:
                break  # Found a match, move to next column
    
    if not column_mapping:
        print("\n[WARNING] Could not auto-map columns. Manual mapping needed.")
    else:
        print(f"\n[OK] Successfully mapped {len(column_mapping)} columns")
    
    return df_our, df_xilnex, column_mapping

def create_match_key(df, product_col, category_col, store_col, sale_type_col):
    """
    Create a unique key for matching rows between datasets.
    Key format: "PRODUCT|CATEGORY|STORE|SALETYPE"
    """
    df['match_key'] = (
        df[product_col].fillna('UNKNOWN').astype(str).str.strip().str.upper() + '|' +
        df[category_col].fillna('UNKNOWN').astype(str).str.strip().str.upper() + '|' +
        df[store_col].fillna('UNKNOWN').astype(str).str.strip().str.upper() + '|' +
        df[sale_type_col].fillna('UNKNOWN').astype(str).str.strip().str.upper()
    )
    return df

def compare_row_counts(df_our, df_xilnex):
    """Compare basic row counts and data completeness"""
    print("\n" + "="*80)
    print("STEP 4: ROW COUNT COMPARISON")
    print("="*80)
    
    our_count = len(df_our)
    xilnex_count = len(df_xilnex)
    difference = our_count - xilnex_count
    pct_diff = (difference / xilnex_count * 100) if xilnex_count > 0 else 0
    
    comparison_data = [
        ["Our Portal", f"{our_count:,}"],
        ["Xilnex", f"{xilnex_count:,}"],
        ["Difference", f"{difference:,} ({pct_diff:+.2f}%)"]
    ]
    
    print("\n" + tabulate(comparison_data, headers=["Source", "Row Count"], tablefmt="grid"))
    
    if abs(pct_diff) > 5:
        print(f"\n[WARNING] Row count difference exceeds 5%!")
    elif difference == 0:
        print(f"\n[OK] Row counts match perfectly!")
    else:
        print(f"\n[INFO] Row count difference: {abs(difference):,} rows")
    
    return {
        'our_count': our_count,
        'xilnex_count': xilnex_count,
        'difference': difference,
        'pct_diff': pct_diff
    }

def compare_summary_totals(df_our, df_xilnex, column_mapping):
    """Compare summary totals between both reports"""
    print("\n" + "="*80)
    print("STEP 5: SUMMARY TOTALS COMPARISON")
    print("="*80)
    
    if not column_mapping:
        print("\n[WARNING] No column mapping available. Skipping totals comparison.")
        return {}
    
    # Reverse mapping: our_col -> xilnex_col
    reverse_map = {v: k for k, v in column_mapping.items()}
    
    results = []
    
    # Compare key metrics
    metrics = {
        'Quantity Sold': 'Quantity Sold',
        'Net Sold Price (RM)': 'Net Sold Price (RM)',
        'Total Cost (RM)': 'Total Cost (RM)',
        'Profit (RM)': 'Profit (RM)'
    }
    
    for metric_name, our_col in metrics.items():
        xilnex_col = reverse_map.get(our_col)
        
        if our_col in df_our.columns and xilnex_col and xilnex_col in df_xilnex.columns:
            our_total = df_our[our_col].sum()
            xilnex_total = df_xilnex[xilnex_col].sum()
            difference = our_total - xilnex_total
            pct_diff = (difference / xilnex_total * 100) if xilnex_total != 0 else 0
            
            results.append([
                metric_name,
                f"{our_total:,.2f}",
                f"{xilnex_total:,.2f}",
                f"{difference:+,.2f} ({pct_diff:+.2f}%)"
            ])
        else:
            results.append([metric_name, "N/A", "N/A", "Column not found"])
    
    if results:
        print("\n" + tabulate(results, 
                             headers=["Metric", "Our Portal", "Xilnex", "Difference"],
                             tablefmt="grid"))
    
    return {}

def find_unmatched_rows(df_our, df_xilnex, column_mapping):
    """Find rows that exist in one dataset but not the other"""
    print("\n" + "="*80)
    print("STEP 6: FINDING UNMATCHED ROWS")
    print("="*80)
    
    if not column_mapping:
        print("\n[WARNING] No column mapping. Skipping unmatched row detection.")
        return {}, {}
    
    # Reverse mapping
    reverse_map = {v: k for k, v in column_mapping.items()}
    
    # Get column names for matching
    our_product = 'Product Name'
    our_category = 'Category'
    our_sale_type = 'Sale Type'
    
    xilnex_product = reverse_map.get(our_product)
    xilnex_category = reverse_map.get(our_category)
    xilnex_sale_type = reverse_map.get(our_sale_type)
    
    if not all([xilnex_product, xilnex_category, xilnex_sale_type]):
        print("\n[WARNING] Required columns for matching not found.")
        return {}, {}
    
    # Create match keys
    df_our = create_match_key(df_our, our_product, our_category, 'Store', our_sale_type)
    df_xilnex = create_match_key(df_xilnex, xilnex_product, xilnex_category, 
                                  reverse_map.get('Store', df_xilnex.columns[0]), 
                                  xilnex_sale_type)
    
    # Find unmatched
    our_keys = set(df_our['match_key'])
    xilnex_keys = set(df_xilnex['match_key'])
    
    only_in_our = our_keys - xilnex_keys
    only_in_xilnex = xilnex_keys - our_keys
    
    print(f"\n[INFO] Rows only in our portal: {len(only_in_our)}")
    print(f"[INFO] Rows only in Xilnex: {len(only_in_xilnex)}")
    
    if len(only_in_our) > 0:
        print(f"\nSample rows only in our portal (first 10):")
        sample_our = df_our[df_our['match_key'].isin(list(only_in_our)[:10])]
        for _, row in sample_our.iterrows():
            print(f"  - {row['Product Name']} | {row['Category']} | {row['Sale Type']}")
    
    if len(only_in_xilnex) > 0:
        print(f"\nSample rows only in Xilnex (first 10):")
        sample_xilnex = df_xilnex[df_xilnex['match_key'].isin(list(only_in_xilnex)[:10])]
        for _, row in sample_xilnex.iterrows():
            print(f"  - {row[xilnex_product]} | {row[xilnex_category]} | {row[xilnex_sale_type]}")
    
    return only_in_our, only_in_xilnex

def compare_matched_rows(df_our, df_xilnex):
    """Compare metrics for rows that exist in both datasets"""
    print("\n" + "="*80)
    print("STEP 7: COMPARING MATCHED ROWS")
    print("="*80)
    
    print("\n[INFO] Will compare matched rows once column mapping is complete")
    print("[INFO] Will check for differences in:")
    print("  - Quantity Sold")
    print("  - Unit Sold Price")
    print("  - Net Sold Price")
    print("  - Total Cost")
    print("  - Profit")
    print("  - Profit Margin")
    
    return []

def generate_comparison_report(results):
    """Generate a detailed comparison report"""
    print("\n" + "="*80)
    print("COMPARISON REPORT SUMMARY")
    print("="*80)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\nGenerated: {timestamp}")
    print(f"Period: September 2025")
    
    print("\n[INFO] Full comparison report will be generated after all comparisons")
    
    # Save detailed report to file
    output_file = Path("product_mix_comparison_report.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("PRODUCT MIX REPORT COMPARISON - DETAILED REPORT\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"Period: September 2025\n")
        f.write(f"Our Portal: {OUR_PORTAL_FILE}\n")
        f.write(f"Xilnex: {XILNEX_FILE}\n")
        f.write("\n[INFO] Detailed comparison will be written after analysis\n")
    
    print(f"\n[OK] Report will be saved to: {output_file}")

def main():
    """Main comparison workflow"""
    print("\n" + "="*80)
    print("PRODUCT MIX REPORT COMPARISON")
    print("Our Portal vs Xilnex Portal")
    print("Period: September 2025")
    print("="*80)
    
    try:
        # Step 1: Load our portal data
        df_our = load_our_portal_data()
        
        # Step 2: Load Xilnex data
        df_xilnex = load_xilnex_data()
        
        # Step 3: Normalize column names
        df_our, df_xilnex, column_mapping = normalize_column_names(df_our, df_xilnex)
        
        # Step 4: Compare row counts
        row_count_results = compare_row_counts(df_our, df_xilnex)
        
        # Step 5: Compare summary totals
        summary_results = compare_summary_totals(df_our, df_xilnex, column_mapping)
        
        # Step 6: Find unmatched rows
        our_unmatched, xilnex_unmatched = find_unmatched_rows(df_our, df_xilnex, column_mapping)
        
        # Step 7: Compare matched rows
        mismatches = compare_matched_rows(df_our, df_xilnex)
        
        # Step 8: Generate report
        results = {
            'row_counts': row_count_results,
            'summary': summary_results,
            'our_unmatched': our_unmatched,
            'xilnex_unmatched': xilnex_unmatched,
            'mismatches': mismatches
        }
        generate_comparison_report(results)
        
        print("\n" + "="*80)
        print("COMPARISON COMPLETE!")
        print("="*80)
        print("\n[OK] Initial inspection complete.")
        print("[INFO] Script will be enhanced once column structures are confirmed.")
        
    except FileNotFoundError as e:
        print(f"\n[ERROR] File not found: {e}")
        print("\nPlease verify file paths:")
        print(f"  Our Portal: {OUR_PORTAL_FILE}")
        print(f"  Xilnex: {XILNEX_FILE}")
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

