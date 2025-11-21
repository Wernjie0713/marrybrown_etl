"""
Test Script for Optimized ETL Pipeline
Tests with a small date range for development/validation

This script demonstrates the optimized approach:
1. Extract with smart early exit (limited API calls)
2. Upsert to staging (safe, no duplicates)
3. MERGE to facts (atomic, no data loss)

Author: YONG WERN JIE
Date: November 7, 2025
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api import (
    extract_sales_for_period_smart,
    save_raw_json,
    load_to_staging_upsert
)
from api_etl.transform_api_to_facts import transform_to_facts_optimized
from dotenv import load_dotenv

# Load environment
load_dotenv('.env.cloud')


def test_optimized_etl_small_range():
    """
    Test optimized ETL with a small date range (1 week)
    
    This is perfect for:
    - Development testing
    - Validating the optimized approach
    - Quick iterations
    """
    print()
    print("╔═════════════════════════════════════════════════════════════════╗")
    print("║           TEST: OPTIMIZED ETL (SMALL DATE RANGE)               ║")
    print("╚═════════════════════════════════════════════════════════════════╝")
    print()
    
    # Test Configuration - Just 1 week
    START_DATE = "2018-10-01"
    END_DATE = "2018-10-07"  # First week of October 2018
    
    print(f"  Test Date Range: {START_DATE} to {END_DATE}")
    print(f"  Expected API Calls: ~10 calls (with smart exit)")
    print(f"  Expected Records: ~7,000 sales")
    print()
    
    overall_start = datetime.now()
    
    try:
        # ============================================================
        # PHASE 1: EXTRACTION
        # ============================================================
        print("="*80)
        print("PHASE 1: EXTRACTION (with smart early exit)")
        print("="*80)
        print()
        
        sales = extract_sales_for_period_smart(
            START_DATE,
            END_DATE,
            enable_early_exit=True,
            buffer_days=7
        )
        
        if not sales:
            print("[ERROR] No data extracted")
            return False
        
        print(f"\n[OK] Extracted {len(sales):,} sales\n")
        
        # ============================================================
        # PHASE 2: STAGING UPSERT
        # ============================================================
        print("="*80)
        print("PHASE 2: STAGING UPSERT (no duplicates)")
        print("="*80)
        print()
        
        load_to_staging_upsert(sales, START_DATE, END_DATE)
        
        print("\n[OK] Staging load complete\n")
        
        # ============================================================
        # PHASE 3: FACT TRANSFORM (MERGE)
        # ============================================================
        print("="*80)
        print("PHASE 3: FACT TRANSFORM (MERGE)")
        print("="*80)
        print()
        
        transform_to_facts_optimized()
        
        print("\n[OK] Fact transform complete\n")
        
        # ============================================================
        # SUCCESS
        # ============================================================
        overall_end = datetime.now()
        duration = (overall_end - overall_start).total_seconds()
        
        print()
        print("="*80)
        print(" "*25 + "TEST PASSED! ✓")
        print("="*80)
        print()
        print(f"  Date Range: {START_DATE} to {END_DATE}")
        print(f"  Total Time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"  Records Processed: {len(sales):,}")
        print()
        print("  Validation Steps:")
        print("    1. Check staging tables for data")
        print("    2. Check fact_sales_transactions for new records")
        print("    3. Verify no duplicates exist")
        print("    4. Run this test again to verify upsert works (no duplicates)")
        print()
        
        return True
        
    except Exception as e:
        print()
        print(f"[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_optimized_etl_single_month():
    """
    Test optimized ETL with full October 2018 (for comparison)
    """
    print()
    print("╔═════════════════════════════════════════════════════════════════╗")
    print("║         TEST: OPTIMIZED ETL (FULL OCTOBER 2018)                ║")
    print("╚═════════════════════════════════════════════════════════════════╝")
    print()
    
    START_DATE = "2018-10-01"
    END_DATE = "2018-10-31"
    
    print(f"  Test Date Range: {START_DATE} to {END_DATE}")
    print(f"  Expected API Calls: ~50 calls (with smart exit)")
    print(f"  Expected Records: ~30,000 sales")
    print()
    
    overall_start = datetime.now()
    
    try:
        # Extract
        print("="*80)
        print("PHASE 1: EXTRACTION")
        print("="*80)
        print()
        
        sales = extract_sales_for_period_smart(START_DATE, END_DATE)
        
        if not sales:
            print("[ERROR] No data extracted")
            return False
        
        print(f"\n[OK] Extracted {len(sales):,} sales\n")
        
        # Save JSON backup
        save_raw_json(sales, "October_2018_optimized_test")
        
        # Upsert to staging
        print("="*80)
        print("PHASE 2: STAGING UPSERT")
        print("="*80)
        print()
        
        load_to_staging_upsert(sales, START_DATE, END_DATE)
        
        # Transform to facts
        print("="*80)
        print("PHASE 3: FACT TRANSFORM")
        print("="*80)
        print()
        
        transform_to_facts_optimized()
        
        # Success
        overall_end = datetime.now()
        duration = (overall_end - overall_start).total_seconds()
        
        print()
        print("="*80)
        print(" "*25 + "TEST PASSED! ✓")
        print("="*80)
        print()
        print(f"  Date Range: {START_DATE} to {END_DATE}")
        print(f"  Total Time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"  Records: {len(sales):,}")
        print()
        
        return True
        
    except Exception as e:
        print()
        print(f"[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Main test runner - choose which test to run
    """
    print()
    print("Choose a test:")
    print("  1. Small range test (1 week - fastest)")
    print("  2. Full month test (October 2018)")
    print("  3. Both tests")
    print()
    
    choice = input("Enter choice (1-3, or press Enter for default=1): ").strip()
    
    if not choice:
        choice = "1"
    
    print()
    
    if choice == "1":
        success = test_optimized_etl_small_range()
    elif choice == "2":
        success = test_optimized_etl_single_month()
    elif choice == "3":
        print("Running both tests...\n")
        success1 = test_optimized_etl_small_range()
        if success1:
            print("\n" + "="*80 + "\n")
            success2 = test_optimized_etl_single_month()
            success = success1 and success2
        else:
            success = False
    else:
        print("[ERROR] Invalid choice")
        sys.exit(1)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Test stopped by user")
        sys.exit(1)

