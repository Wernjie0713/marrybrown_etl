#!/usr/bin/env python3
"""
Quick Test Script for Date Range Fix
Tests the broadened date range to ensure data flows through
"""

import sys
import os
from datetime import datetime

# Override chunk size limits for testing - MUST be set before importing ETL modules
# This allows chunk_size=1 to actually work (default min is 25)
os.environ["CHUNK_MIN_SIZE"] = "1"
os.environ["CHUNK_MAX_SIZE"] = "1"  # Prevent adaptive growth during test

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.run_cloud_etl_chunked import run_chunked_etl

def test_date_range_fix():
    """Test the date range fix with a very small chunk"""
    print("="*80)
    print("TESTING DATE RANGE FIX")
    print("="*80)
    print()
    print("This test will:")
    print("1. Use broadened date range (2018-2025)")
    print("2. Run only 1 chunk (1 API call) for quick testing")
    print("   (Chunk size limits overridden: MIN=1, MAX=1)")
    print("3. Show debug output with actual API dates")
    print("4. Verify data flows to staging tables")
    print()
    
    # Test with minimal data for fast debugging
    CHUNK_SIZE = 1  # Only 1 API call (1000 records) for speed testing
    
    # Test with very small chunk for quick verification
    success = run_chunked_etl(
        start_date="2018-01-01",  # Very broad range
        end_date="2025-12-31",    # Very broad range
        chunk_size=CHUNK_SIZE,            # Small chunk for quick test
        enable_early_exit=False,  # Disable smart exit for testing
        buffer_days=0,
        resume=False,             # Start fresh
        force_restart=True        # Clear any existing progress
    )
    
    print()
    print("="*80)
    if success:
        print("✅ SUCCESS: Date range fix works!")
        print("   - Data should now flow to staging tables")
        print("   - Check debug output above for actual API dates")
    else:
        print("❌ FAILED: Still having issues")
        print("   - Check debug output for date format problems")
        print("   - Verify database connectivity (VPN)")
    print("="*80)
    
    return success

if __name__ == "__main__":
    test_date_range_fix()
