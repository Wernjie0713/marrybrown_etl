"""
Cloud ETL Orchestrator - OPTIMIZED BATCH APPROACH
Extracts sales data with smart early exit, upserts to staging, transforms with MERGE

KEY IMPROVEMENTS:
1. Smart early exit - stops API calls when sufficient data collected
2. Batch extraction - fetches entire date range at once (not month-by-month)
3. Upsert to staging - no duplicates, safe append mode
4. MERGE to facts - atomic upsert, no DELETE+INSERT
5. Single transaction - best performance and safety

Author: YONG WERN JIE
Date: November 7, 2025
"""

import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path to import from api_etl
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api import (
    extract_sales_for_period_smart, 
    save_raw_json, 
    load_to_staging_upsert
)
from api_etl.transform_api_to_facts import transform_to_facts_optimized

# Load environment
load_dotenv('.env.cloud')


def run_optimized_etl(start_date, end_date, enable_early_exit=True, buffer_days=7):
    """
    Run optimized ETL pipeline for specified date range
    
    Pipeline:
    1. Extract from API with smart early exit
    2. Upsert to staging (deduplication handled)
    3. Transform to facts with MERGE (deduplication handled)
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        enable_early_exit: Enable smart API exit (default True)
        buffer_days: Days to fetch past end date (default 7)
    """
    overall_start = datetime.now()
    
    print()
    print("="*80)
    print(" "*20 + "OPTIMIZED CLOUD ETL PIPELINE")
    print(" "*20 + "Batch Extraction → Upsert → MERGE")
    print("="*80)
    print()
    print(f"  Target Date Range: {start_date} to {end_date}")
    print(f"  Smart Early Exit: {'ENABLED' if enable_early_exit else 'DISABLED'}")
    if enable_early_exit:
        print(f"  Buffer Days: {buffer_days}")
    print()
    print("  Strategy:")
    print("    1. Extract entire range with smart stopping")
    print("    2. Upsert to staging (no duplicates)")
    print("    3. MERGE to facts (atomic upsert)")
    print()
    
    try:
        # ============================================================
        # PHASE 1: EXTRACTION WITH SMART EARLY EXIT
        # ============================================================
        print()
        print("="*80)
        print(" "*25 + "PHASE 1: EXTRACTION")
        print("="*80)
        print()
        
        sales = extract_sales_for_period_smart(
            start_date, 
            end_date,
            enable_early_exit=enable_early_exit,
            buffer_days=buffer_days
        )
        
        if not sales:
            print()
            print("[ERROR] No data extracted from API")
            print()
            return False
        
        # Save raw JSON for reference
        print("[OPTIONAL] Saving raw JSON backup...")
        date_identifier = f"{start_date.replace('-', '')}_to_{end_date.replace('-', '')}"
        json_file = save_raw_json(sales, date_identifier)
        print()
        
        # ============================================================
        # PHASE 2: UPSERT TO STAGING
        # ============================================================
        print()
        print("="*80)
        print(" "*25 + "PHASE 2: STAGING LOAD")
        print("="*80)
        print()
        
        load_to_staging_upsert(sales, start_date, end_date)
        
        # ============================================================
        # PHASE 3: TRANSFORM TO FACTS (MERGE)
        # ============================================================
        print()
        print("="*80)
        print(" "*25 + "PHASE 3: FACT TRANSFORM")
        print("="*80)
        print()
        
        transform_to_facts_optimized(start_date, end_date)
        
        # ============================================================
        # SUCCESS SUMMARY
        # ============================================================
        overall_end = datetime.now()
        duration = (overall_end - overall_start).total_seconds()
        
        print()
        print("="*80)
        print(" "*25 + "ETL COMPLETE!")
        print("="*80)
        print()
        print(f"  Date Range: {start_date} to {end_date}")
        print(f"  Total Time: {duration/60:.2f} minutes")
        print(f"  Raw Data: {len(sales):,} sales extracted")
        print(f"  JSON Backup: {json_file}")
        print()
        print("  Next Steps:")
        print("    - Verify data in fact_sales_transactions")
        print("    - Run validation queries")
        print("    - Test FastAPI endpoints")
        print()
        
        return True
        
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] ETL stopped by user")
        return False
        
    except Exception as e:
        print()
        print(f"[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main entry point with configurable date ranges
    
    Default: Oct 2018 - Dec 2019 (15 months)
    For testing: Use smaller ranges like one month
    """
    print()
    print("╔═════════════════════════════════════════════════════════════════╗")
    print("║          OPTIMIZED CLOUD ETL - CONFIGURATION                    ║")
    print("╚═════════════════════════════════════════════════════════════════╝")
    print()
    
    # Configuration
    # ==============
    # For PRODUCTION (full 15 months):
    START_DATE = "2018-10-01"
    END_DATE = "2019-12-31"
    
    # For DEVELOPMENT/TESTING (smaller range):
    # START_DATE = "2018-10-01"
    # END_DATE = "2018-10-31"  # Just October 2018
    
    # Smart exit configuration
    ENABLE_EARLY_EXIT = True
    BUFFER_DAYS = 7  # Fetch 7 days past end date to ensure coverage
    
    print(f"  Start Date: {START_DATE}")
    print(f"  End Date: {END_DATE}")
    print(f"  Smart Exit: {'YES' if ENABLE_EARLY_EXIT else 'NO'}")
    print(f"  Buffer Days: {BUFFER_DAYS}")
    print()
    
    # Confirmation
    print("This will:")
    print("  1. Extract sales data from Xilnex API")
    print("  2. Upsert to staging tables (safe, no duplicates)")
    print("  3. MERGE to fact table (atomic, no data loss)")
    print()
    print("[INFO] Starting in 5 seconds...")
    print("[INFO] Press Ctrl+C to cancel...")
    
    import time
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print()
        print("[CANCELLED] User interrupted startup")
        sys.exit(0)
    
    # Run ETL
    success = run_optimized_etl(
        START_DATE, 
        END_DATE,
        enable_early_exit=ENABLE_EARLY_EXIT,
        buffer_days=BUFFER_DAYS
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

