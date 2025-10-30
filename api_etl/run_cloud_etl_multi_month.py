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

# Add parent directory to path to import from api_etl
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_etl.extract_from_api import extract_sales_for_period, save_raw_json, load_to_staging
from api_etl.transform_api_to_facts import transform_to_facts


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
    """
    print()
    print("╔" + "="*78 + "╗")
    print("║" + " "*20 + "CLOUD ETL - MULTI-MONTH EXTRACTION" + " "*24 + "║")
    print("║" + " "*20 + "October 2018 - December 2019" + " "*30 + "║")
    print("╚" + "="*78 + "╝")
    print()
    
    overall_start = datetime.now()
    
    # Generate month ranges (Oct 2018 - Dec 2019)
    month_ranges = generate_month_ranges(
        start_month=10, start_year=2018,
        end_month=12, end_year=2019
    )
    
    print(f"Total months to extract: {len(month_ranges)}")
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
    
    # Transform to fact table
    if total_sales > 0:
        print()
        print("="*80)
        print("STARTING TRANSFORMATION TO FACT TABLE")
        print("="*80)
        print()
        
        try:
            transform_to_facts()
            print()
            print("[OK] Transformation complete!")
            print()
        except Exception as e:
            print(f"[ERROR] Transformation failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("[WARNING] No sales to transform. Skipping fact table load.")
    
    # Summary
    overall_end = datetime.now()
    duration = (overall_end - overall_start).total_seconds()
    
    print()
    print("╔" + "="*78 + "╗")
    print("║" + " "*25 + "ETL COMPLETE!" + " "*41 + "║")
    print("╚" + "="*78 + "╝")
    print()
    print(f"  Total Time: {duration/60:.1f} minutes")
    print(f"  Months Processed: {successful_months}")
    print(f"  Sales Extracted: {total_sales:,}")
    print()
    print("Next steps:")
    print("  1. Run dimension table ETL scripts if needed")
    print("  2. Verify data in fact_sales_transactions")
    print("  3. Test FastAPI backend connection to cloud warehouse")
    print("  4. Deploy portal and run validation tests")
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

