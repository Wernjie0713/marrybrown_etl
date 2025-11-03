"""
Cloud ETL Orchestrator - Multi-Month Extraction
Extracts sales data from Oct 2018 to Dec 2019 (15 months)

Author: YONG WERN JIE
Date: October 29, 2025
"""

import sys
import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pyodbc
from dotenv import load_dotenv

# Add parent directory to path to import from api_etl
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api import extract_sales_for_period, save_raw_json, load_to_staging
from api_etl.transform_api_to_facts import transform_to_facts_for_period

# Load environment
load_dotenv('.env.cloud')


def get_warehouse_connection():
    """Create connection to cloud warehouse for checking existing data"""
    driver = os.getenv('TARGET_DRIVER', 'ODBC Driver 18 for SQL Server')
    server = os.getenv('TARGET_SERVER')
    database = os.getenv('TARGET_DATABASE')
    username = os.getenv('TARGET_USERNAME')
    password = os.getenv('TARGET_PASSWORD')
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
        f"Timeout=30;"
    )
    
    return pyodbc.connect(conn_str)


def check_existing_months():
    """
    Check which months already exist in fact_sales_transactions
    
    Returns:
        set: Set of month strings in format 'YYYY-MM'
    """
    try:
        conn = get_warehouse_connection()
        cursor = conn.cursor()
        
        # Query distinct year-month combinations from fact table
        cursor.execute("""
            SELECT DISTINCT LEFT(CAST(DateKey AS VARCHAR), 6) AS YearMonth
            FROM dbo.fact_sales_transactions
            WHERE DateKey IS NOT NULL
            ORDER BY YearMonth
        """)
        
        existing_months = set()
        for row in cursor.fetchall():
            year_month = row.YearMonth  # e.g., '201810'
            # Convert to 'YYYY-MM' format
            formatted = f"{year_month[:4]}-{year_month[4:6]}"
            existing_months.add(formatted)
        
        cursor.close()
        conn.close()
        
        return existing_months
        
    except Exception as e:
        print(f"[WARNING] Could not check existing months: {e}")
        print("[INFO] Proceeding with all months (no skip)...")
        return set()


def generate_month_ranges(start_month, start_year, end_month, end_year):
    """
    Generate list of (start_date, end_date) tuples for each month in range
    
    Args:
        start_month: Starting month (1-12)
        start_year: Starting year
        end_month: Ending month (1-12)
        end_year: Ending year
        
    Returns:
        List of tuples: [(start_date_str, end_date_str), ...]
    """
    ranges = []
    current = date(start_year, start_month, 1)
    end = date(end_year, end_month, 1)
    
    while current <= end:
        # First day of month
        month_start = current
        
        # Last day of month
        next_month = current + relativedelta(months=1)
        month_end = next_month - relativedelta(days=1)
        
        ranges.append((
            month_start.strftime('%Y-%m-%d'),
            month_end.strftime('%Y-%m-%d'),
            month_start.strftime('%B %Y')  # Human-readable name
        ))
        
        current = next_month
    
    return ranges


def extract_month(start_date, end_date, month_name):
    """
    Extract sales data for a single month
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        month_name: Human-readable month name
    """
    print()
    print("="*80)
    print(f"EXTRACTING: {month_name}")
    print(f"Date Range: {start_date} to {end_date}")
    print("="*80)
    print()
    
    try:
        # Extract sales from API
        sales = extract_sales_for_period(start_date, end_date)
        
        if not sales:
            print(f"[WARNING] No sales found for {month_name}")
            return 0
        
        # Save raw JSON for reference
        json_file = save_raw_json(sales, month_name.replace(' ', '_'))
        
        # Load to staging tables
        load_to_staging(sales)

        # Transform to facts for this month window (idempotent per window)
        print("[INFO] Transforming month to facts...")
        transform_to_facts_for_period(start_date, end_date)
        
        print()
        print(f"[OK] {month_name} extraction complete: {len(sales)} sales")
        print()
        
        return len(sales)
        
    except Exception as e:
        print(f"[ERROR] Failed to extract {month_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0


def main():
    """
    Main orchestration function
    Extracts Oct 2018 - Dec 2019 (15 months) and transforms to fact table
    WITH SMART RESUME: Automatically skips already-loaded months
    """
    print()
    print("="*80)
    print(" "*20 + "CLOUD ETL - MULTI-MONTH EXTRACTION")
    print(" "*20 + "October 2018 - December 2019")
    print(" "*20 + "WITH SMART RESUME CAPABILITY")
    print("="*80)
    print()
    
    overall_start = datetime.now()
    
    # Generate all expected month ranges (Oct 2018 - Dec 2019)
    all_month_ranges = generate_month_ranges(
        start_month=10, start_year=2018,
        end_month=12, end_year=2019
    )
    
    print(f"[STEP 1] Checking for already-loaded months...")
    print()
    
    # Check which months already exist
    existing_months = check_existing_months()
    
    if existing_months:
        print(f"[INFO] Found {len(existing_months)} months already in warehouse:")
        for month in sorted(existing_months):
            print(f"  - {month} (SKIP)")
        print()
    else:
        print("[INFO] No existing data found. Starting from scratch.")
        print()
    
    # Filter out months that already exist
    month_ranges = []
    skipped_count = 0
    
    for start_date, end_date, month_name in all_month_ranges:
        # Extract YYYY-MM from month_name (e.g., "October 2018" -> "2018-10")
        month_parts = month_name.split()
        month_num = datetime.strptime(month_parts[0], '%B').month
        year_num = int(month_parts[1])
        month_key = f"{year_num}-{month_num:02d}"
        
        if month_key in existing_months:
            skipped_count += 1
        else:
            month_ranges.append((start_date, end_date, month_name))
    
    print("="*80)
    print(f"RESUME STRATEGY:")
    print(f"  Total Expected: {len(all_month_ranges)} months")
    print(f"  Already Loaded: {skipped_count} months (SKIP)")
    print(f"  To Process: {len(month_ranges)} months")
    print("="*80)
    print()
    
    if len(month_ranges) == 0:
        print("[SUCCESS] All months already loaded! Nothing to do.")
        print()
        return
    
    # Show what will be processed
    print("Months to process:")
    for i, (_, _, month_name) in enumerate(month_ranges, 1):
        print(f"  {i:2d}. {month_name}")
    print()
    
    # Give user a chance to review
    print("[INFO] Starting extraction in 5 seconds...")
    print("[INFO] Press Ctrl+C to cancel if needed...")
    import time
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print()
        print("[CANCELLED] User interrupted startup")
        return
    
    print()
    print("="*80)
    print("STARTING EXTRACTION...")
    print("="*80)
    print()
    
    # Track progress
    total_sales = 0
    successful_months = 0
    failed_months = []
    
    # Extract each month sequentially
    for i, (start_date, end_date, month_name) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_name}...")
        
        sales_count = extract_month(start_date, end_date, month_name)
        
        if sales_count > 0:
            total_sales += sales_count
            successful_months += 1
        else:
            failed_months.append(month_name)
    
    print()
    print("="*80)
    print("EXTRACTION PHASE COMPLETE")
    print("="*80)
    print(f"  Successful Months: {successful_months}/{len(month_ranges)}")
    print(f"  Total Sales Extracted: {total_sales:,}")
    if failed_months:
        print(f"  Failed Months: {', '.join(failed_months)}")
    print()
    
    # Transform was executed per-month above
    if total_sales == 0:
        print("[WARNING] No sales extracted. No fact loads were performed.")
    
    # Summary
    overall_end = datetime.now()
    duration = (overall_end - overall_start).total_seconds()
    
    print()
    print("="*80)
    print(" "*25 + "ETL COMPLETE!")
    print("="*80)
    print()
    print(f"  Total Time: {duration/60:.1f} minutes")
    print(f"  Months Skipped (already loaded): {skipped_count}")
    print(f"  Months Processed (new): {successful_months}")
    print(f"  Total Coverage: {skipped_count + successful_months}/{len(all_month_ranges)} months")
    print(f"  Sales Extracted (new): {total_sales:,}")
    if failed_months:
        print(f"  Failed Months: {', '.join(failed_months)}")
    print()
    print("Next steps:")
    print("  1. Run check_cloud_etl_status.py to verify all 15 months loaded")
    print("  2. Test FastAPI backend connection to cloud warehouse")
    print("  3. Deploy portal and run validation tests")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] ETL stopped by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

