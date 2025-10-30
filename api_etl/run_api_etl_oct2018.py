"""
Main Orchestration Script for API ETL (October 2018)
Runs extraction and transformation in sequence

Author: YONG WERN JIE
Date: October 28, 2025
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api import extract_october_2018, save_raw_json, load_to_staging
from api_etl.transform_api_to_facts import transform_to_facts


def main():
    """Main orchestration function"""
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║           XILNEX API ETL - OCTOBER 2018                        ║")
    print("║           Complete Pipeline Execution                         ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()
    
    try:
        # Step 1: Extract from API
        print("[STEP 1/4] Extracting from Xilnex Sync API...")
        print()
        sales = extract_october_2018()
        
        if not sales:
            print()
            print("[ERROR] No sales data retrieved. Exiting.")
            return 1
        
        print()
        print(f"[SUCCESS] Extracted {len(sales)} sales from API")
        print()
        input("Press Enter to continue to Step 2...")
        print()
        
        # Step 2: Save raw JSON
        print("[STEP 2/4] Saving raw JSON for reference...")
        print()
        json_file = save_raw_json(sales)
        print(f"[SUCCESS] Saved to {json_file}")
        print()
        input("Press Enter to continue to Step 3...")
        print()
        
        # Step 3: Load to staging
        print("[STEP 3/4] Loading to staging tables...")
        print()
        load_to_staging(sales)
        print("[SUCCESS] Data loaded to staging_sales_api, staging_sales_items_api, staging_payments_api")
        print()
        input("Press Enter to continue to Step 4...")
        print()
        
        # Step 4: Transform to facts
        print("[STEP 4/4] Transforming to fact_sales_transactions_api...")
        print()
        transform_to_facts()
        print("[SUCCESS] Data transformed and loaded to fact_sales_transactions_api")
        print()
        
        # Final summary
        print()
        print("╔════════════════════════════════════════════════════════════════╗")
        print("║                 ETL PIPELINE COMPLETE!                         ║")
        print("╚════════════════════════════════════════════════════════════════╝")
        print()
        print("✓ Step 1: API Extraction Complete")
        print("✓ Step 2: Raw JSON Saved")
        print("✓ Step 3: Staging Tables Loaded")
        print("✓ Step 4: Fact Table Populated")
        print()
        print("Next Steps:")
        print("  1. cd ../marrybrown_api")
        print("  2. uvicorn main:app --reload")
        print("  3. Access: http://localhost:8000/reports/daily-sales-api-test")
        print("  4. Export to Excel and compare with Xilnex portal")
        print()
        
        return 0
        
    except KeyboardInterrupt:
        print()
        print()
        print("[CANCELLED] ETL pipeline interrupted by user")
        print()
        return 1
        
    except Exception as e:
        print()
        print()
        print(f"[ERROR] ETL pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        print()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

