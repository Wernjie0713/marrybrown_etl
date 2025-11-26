"""
Cloud ETL Orchestrator - CHUNKED APPROACH
Extracts and loads in chunks for maximum safety and progress preservation

KEY BENEFITS:
1. Progress saved every 50 API calls (~50K records)
2. Memory efficient - clears after each chunk
3. Crash-safe - can resume from last saved chunk
4. Early failure detection - problems found quickly
5. No risk of "800 calls then fail at TL"

Author: YONG WERN JIE
Date: November 7, 2025
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api_chunked import extract_and_load_chunked, clear_progress
from api_etl.transform_api_to_facts import transform_to_facts_optimized
from utils.env_loader import load_environment

# Load environment - use .env.local for local development
load_environment(force_local=True)


def run_chunked_etl(start_date, end_date, chunk_size=50, enable_early_exit=True, 
                    buffer_days=7, resume=True, force_restart=False):
    """
    Run chunked ETL pipeline for specified date range
    
    Pipeline:
    1. Extract from API in chunks of CHUNK_SIZE calls
    2. Load each chunk to staging immediately (MERGE for safety)
    3. Clear memory after each chunk
    4. Transform all staging data to facts (MERGE)
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        chunk_size: Number of API calls per chunk (default 50)
        enable_early_exit: Enable smart API exit (default True)
        buffer_days: Days to fetch past end date (default 7)
        resume: Enable resume from last checkpoint (default True)
        force_restart: Force restart from beginning (default False)
    """
    overall_start = datetime.now()
    
    print()
    print("="*80)
    print(" "*20 + "CHUNKED CLOUD ETL PIPELINE")
    print(" "*20 + "Extract-in-Chunks -> Load -> Transform")
    print("="*80)
    print()
    print(f"  Target Date Range: {start_date} to {end_date}")
    print(f"  Chunk Size: {chunk_size} API calls (~{chunk_size}K records per save)")
    print(f"  Smart Early Exit: {'ENABLED' if enable_early_exit else 'DISABLED'}")
    if enable_early_exit:
        print(f"  Buffer Days: {buffer_days}")
    print()
    print("  Advantages of Chunked Approach:")
    print("    [+] Progress saved every chunk (crash-safe)")
    print("    [+] Memory efficient (cleared after each save)")
    print("    [+] Early failure detection (not after 800 calls)")
    print("    [+] Can resume from last saved chunk")
    print("    [+] No risk of 'extract all then fail at load'")
    print()
    
    try:
        # ============================================================
        # PHASE 1 & 2 COMBINED: CHUNKED EXTRACTION & LOADING
        # ============================================================
        print()
        print("="*80)
        print(" "*20 + "PHASE 1+2: CHUNKED EXTRACT & LOAD")
        print("="*80)
        print()
        
        extraction_stats = extract_and_load_chunked(
            start_date, 
            end_date,
            chunk_size=chunk_size,
            enable_early_exit=enable_early_exit,
            buffer_days=buffer_days,
            resume=resume,
            force_restart=force_restart
        )
        
        if extraction_stats["sales"] == 0:
            print()
            print("[ERROR] No data loaded to staging")
            print()
            return False
        
        # ============================================================
        # PHASE 3: TRANSFORM TO FACTS (MERGE)
        # ============================================================
        print()
        print("="*80)
        print(" "*25 + "PHASE 3: FACT TRANSFORM")
        print("="*80)
        print()
        
        transform_to_facts_optimized()
        
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
        print(f"  API Calls: {extraction_stats['api_calls']}")
        print(f"  Sales Loaded: {extraction_stats['sales']:,}")
        print(f"  Items Loaded: {extraction_stats['items']:,}")
        print(f"  Payments Loaded: {extraction_stats['payments']:,}")
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
        print("[INFO] Progress has been saved to staging tables")
        print("[INFO] You can resume by running this script again")
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
    
    RECOMMENDATION FOR TESTING:
    - Start with 1-3 months to test the chunked approach
    - If successful, extend to full 15 months
    - Chunk size of 50 = saves every ~50K records (~2-3 minutes per chunk)
    """
    print()
    print("="*67)
    print(" "*10 + "CHUNKED CLOUD ETL - CONFIGURATION")
    print("="*67)
    print()
    
    # Configuration
    # ==============
    # TEMPORARY: Broad date range for timestamp-based sync testing
    START_DATE = "2018-01-01"  # Much earlier to capture all data
    END_DATE = "2025-12-31"    # Much later to capture all data
    
    # Original testing range (restore after timestamp sync validated):
    # START_DATE = "2018-10-01"
    # END_DATE = "2018-12-31"
    
    # For full production (15 months) - use after testing:
    # END_DATE = "2019-12-31"
    
    # Chunk configuration
    CHUNK_SIZE = 50  # Save every 50 API calls (~50K records, ~2-3 min per chunk)
    
    # Smart exit configuration
    ENABLE_EARLY_EXIT = True
    BUFFER_DAYS = 7
    
    # Resume configuration
    RESUME = True  # Enable resume from last checkpoint
    FORCE_RESTART = True  # Set to True to force restart from beginning
    
    print(f"  Start Date: {START_DATE}")
    print(f"  End Date: {END_DATE}")
    print(f"  Chunk Size: {CHUNK_SIZE} API calls (~{CHUNK_SIZE}K records per save)")
    print(f"  Smart Exit: {'YES' if ENABLE_EARLY_EXIT else 'NO'}")
    print(f"  Buffer Days: {BUFFER_DAYS}")
    print(f"  Resume Mode: {'ENABLED' if RESUME else 'DISABLED'}")
    print(f"  Force Restart: {'YES (will clear progress)' if FORCE_RESTART else 'NO'}")
    print()
    
    # Confirmation
    print("This will:")
    print(f"  1. Extract sales data from Xilnex API in chunks of {CHUNK_SIZE} calls")
    print("  2. Save each chunk to staging immediately (crash-safe)")
    print("  3. Save progress after each chunk (resumable)")
    print("  4. Clear memory after each chunk (memory efficient)")
    print("  5. Transform all staging data to facts (atomic MERGE)")
    print()
    print("Benefits:")
    print("  [+] Progress preserved even if interrupted")
    print("  [+] Can resume from last checkpoint (no wasted API calls)")
    print("  [+] Early failure detection")
    print("  [+] Memory efficient")
    print("  [+] No risk of 'extract all then fail'")
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
    success = run_chunked_etl(
        START_DATE, 
        END_DATE,
        chunk_size=CHUNK_SIZE,
        enable_early_exit=ENABLE_EARLY_EXIT,
        buffer_days=BUFFER_DAYS,
        resume=RESUME,
        force_restart=FORCE_RESTART
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

