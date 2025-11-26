"""
Cloud ETL - Single Month Test (October 2018)
Quick test to verify cloud ETL works before running full 15 months

Author: YONG WERN JIE
Date: October 30, 2025
"""

import sys
import os

# Add api_etl directory to path
sys.path.insert(0, os.path.dirname(__file__))

from extract_from_api import extract_sales_for_period, load_to_staging, save_raw_json
from transform_api_to_facts import transform_to_facts

def main():
    print("=" * 80)
    print("CLOUD ETL - SINGLE MONTH TEST")
    print("October 2018 Only")
    print("=" * 80)
    print()
    
    try:
        # Extract October 2018
        print("[1/3] EXTRACTING October 2018 from API...")
        sales = extract_sales_for_period('2018-10-01', '2018-10-31')
        print(f"  [OK] Extracted {len(sales)} sales")
        print()
        
        # Save raw JSON
        save_raw_json(sales, 'October_2018')
        
        # Load to staging
        print("[2/3] LOADING to staging tables...")
        load_to_staging(sales)
        print(f"  [OK] Loaded to staging")
        print()
        
        # Transform to fact table
        print("[3/3] TRANSFORMING to fact table...")
        row_count = transform_to_facts()
        print(f"  [OK] Transformed {row_count} rows to fact_sales_transactions")
        print()
        
        print("=" * 80)
        print("SUCCESS! October 2018 ETL Complete")
        print("=" * 80)
        print()
        print("Summary:")
        print(f"  - Sales extracted: {len(sales)}")
        print(f"  - Fact rows created: {row_count}")
        print()
        print("Next step: Run full 15-month ETL after working hours:")
        print("  python api_etl\\run_cloud_etl_multi_month.py")
        print()
        
    except Exception as e:
        print()
        print("=" * 80)
        print("FAILED!")
        print("=" * 80)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

