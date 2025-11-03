"""
Re-extract September 2019 data after fixing extraction logic
This script will extract September 2019 data using the fixed timestamp pagination

Author: YONG WERN JIE
Date: November 4, 2025
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api import extract_sales_for_period, save_raw_json, load_to_staging
from api_etl.transform_api_to_facts import transform_to_facts

# Load environment
from dotenv import load_dotenv
load_dotenv('.env.cloud')


def main():
    """
    Re-extract September 2019 data
    """
    print()
    print("="*80)
    print(" "*25 + "RE-EXTRACT: SEPTEMBER 2019")
    print(" "*25 + "Using Fixed Timestamp Pagination")
    print("="*80)
    print()
    
    start_date = "2019-09-01"
    end_date = "2019-09-30"
    
    print(f"Target Date Range: {start_date} to {end_date}")
    print()
    print("[INFO] This will:")
    print("  1. Fetch ALL data via timestamp pagination")
    print("  2. Filter for September 2019 dates")
    print("  3. Load to staging tables")
    print("  4. Transform to fact table")
    print()
    print("[INFO] Starting in 5 seconds...")
    print("[INFO] Press Ctrl+C to cancel if needed...")
    print()
    
    import time
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print()
        print("[CANCELLED] User interrupted")
        return
    
    print()
    print("="*80)
    print("STEP 1: EXTRACTING FROM API")
    print("="*80)
    print()
    
    # Extract sales from API
    sales = extract_sales_for_period(start_date, end_date)
    
    if not sales:
        print()
        print("[WARNING] No sales returned from API!")
        print()
        print("Possible reasons:")
        print("  1. API credentials expired or invalid")
        print("  2. No sales data exists in Xilnex for September 2019")
        print("  3. Data might have been deleted or archived")
        print()
        return
    
    print()
    print("="*80)
    print("STEP 2: SAVING RAW JSON")
    print("="*80)
    print()
    
    # Save raw JSON for reference
    json_file = save_raw_json(sales, "September_2019_REEXTRACT")
    
    print()
    print("="*80)
    print("STEP 3: LOADING TO STAGING TABLES")
    print("="*80)
    print()
    
    # Load to staging tables
    load_to_staging(sales)
    
    print()
    print("="*80)
    print("STEP 4: TRANSFORMING TO FACT TABLE")
    print("="*80)
    print()
    
    # Transform to fact table
    transform_to_facts()
    
    print()
    print("="*80)
    print(" "*25 + "RE-EXTRACTION COMPLETE!")
    print("="*80)
    print()
    print(f"  Sales Extracted: {len(sales):,}")
    print(f"  Raw JSON Saved: {json_file}")
    print()
    print("Next steps:")
    print("  1. Verify data in staging_sales, staging_sales_items, staging_payments")
    print("  2. Verify data in fact_sales_transactions")
    print("  3. Run: python check_cloud_etl_status.py")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Re-extraction stopped by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

