"""
Clear ETL Data - Reset for Fresh Run
Clears staging tables and fact_sales_transactions for API ETL rerun

Author: YONG WERN JIE
Date: November 7, 2025
"""

import os
import sys
from urllib.parse import quote_plus
from dotenv import load_dotenv
import pyodbc

# Load cloud environment
load_dotenv('.env.cloud')


def get_warehouse_connection():
    """Create connection to cloud warehouse"""
    driver = os.getenv('TARGET_DRIVER', 'ODBC Driver 17 for SQL Server')
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


def check_current_data():
    """Check what data currently exists"""
    print("="*80)
    print("CHECKING CURRENT DATA")
    print("="*80)
    print()
    
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    
    try:
        # Check staging_sales
        cursor.execute("SELECT COUNT(*) FROM dbo.staging_sales")
        staging_sales_count = cursor.fetchone()[0]
        print(f"staging_sales: {staging_sales_count:,} rows")
        
        # Check staging_sales_items
        cursor.execute("SELECT COUNT(*) FROM dbo.staging_sales_items")
        staging_items_count = cursor.fetchone()[0]
        print(f"staging_sales_items: {staging_items_count:,} rows")
        
        # Check staging_payments
        cursor.execute("SELECT COUNT(*) FROM dbo.staging_payments")
        staging_payments_count = cursor.fetchone()[0]
        print(f"staging_payments: {staging_payments_count:,} rows")
        
        # Check fact_sales_transactions
        cursor.execute("SELECT COUNT(*) FROM dbo.fact_sales_transactions")
        fact_count = cursor.fetchone()[0]
        print(f"fact_sales_transactions: {fact_count:,} rows")
        
        # Check date range in fact table
        cursor.execute("""
            SELECT 
                MIN(DateKey) as min_date,
                MAX(DateKey) as max_date,
                COUNT(DISTINCT DateKey) as distinct_dates
            FROM dbo.fact_sales_transactions
            WHERE DateKey IS NOT NULL
        """)
        date_info = cursor.fetchone()
        if date_info and date_info[0]:
            print(f"\nFact Table Date Range:")
            print(f"  Min DateKey: {date_info[0]}")
            print(f"  Max DateKey: {date_info[1]}")
            print(f"  Distinct Dates: {date_info[2]}")
            
            # Show months
            cursor.execute("""
                SELECT DISTINCT 
                    LEFT(CAST(DateKey AS VARCHAR), 6) AS YearMonth,
                    COUNT(*) as row_count
                FROM dbo.fact_sales_transactions
                WHERE DateKey IS NOT NULL
                GROUP BY LEFT(CAST(DateKey AS VARCHAR), 6)
                ORDER BY YearMonth
            """)
            print(f"\nMonths loaded:")
            for row in cursor.fetchall():
                year_month = row[0]
                formatted = f"{year_month[:4]}-{year_month[4:6]}"
                print(f"  {formatted}: {row[1]:,} rows")
        
        print()
        
        return {
            'staging_sales': staging_sales_count,
            'staging_items': staging_items_count,
            'staging_payments': staging_payments_count,
            'fact': fact_count
        }
        
    finally:
        cursor.close()
        conn.close()


def clear_all_data():
    """Clear all staging and fact tables"""
    print("="*80)
    print("CLEARING ALL DATA")
    print("="*80)
    print()
    
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    
    try:
        # Clear staging tables
        print("[1/4] Clearing staging_sales...")
        cursor.execute("TRUNCATE TABLE dbo.staging_sales")
        print("  [OK] staging_sales cleared")
        
        print("[2/4] Clearing staging_sales_items...")
        cursor.execute("TRUNCATE TABLE dbo.staging_sales_items")
        print("  [OK] staging_sales_items cleared")
        
        print("[3/4] Clearing staging_payments...")
        cursor.execute("TRUNCATE TABLE dbo.staging_payments")
        print("  [OK] staging_payments cleared")
        
        print("[4/4] Clearing fact_sales_transactions...")
        cursor.execute("TRUNCATE TABLE dbo.fact_sales_transactions")
        print("  [OK] fact_sales_transactions cleared")
        
        conn.commit()
        
        print()
        print("="*80)
        print("ALL DATA CLEARED SUCCESSFULLY")
        print("="*80)
        print()
        
    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] Failed to clear data: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cursor.close()
        conn.close()


def main():
    """Main execution"""
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║         CLEAR ETL DATA - RESET FOR FRESH RUN                  ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()
    
    try:
        # Step 1: Check current data
        counts = check_current_data()
        
        # Step 2: Confirm deletion
        total_rows = sum(counts.values())
        
        if total_rows == 0:
            print("[INFO] No data found. Tables are already empty.")
            print()
            return
        
        print("="*80)
        print("⚠️  WARNING: This will DELETE ALL data from:")
        print("  - staging_sales")
        print("  - staging_sales_items")
        print("  - staging_payments")
        print("  - fact_sales_transactions")
        print()
        print(f"Total rows to be deleted: {total_rows:,}")
        print("="*80)
        print()
        
        confirmation = input("Type 'YES' to confirm deletion: ")
        
        if confirmation.strip().upper() != 'YES':
            print()
            print("[CANCELLED] Data clearing cancelled by user")
            print()
            return
        
        print()
        
        # Step 3: Clear data
        clear_all_data()
        
        # Step 4: Verify
        print("Verifying deletion...")
        print()
        check_current_data()
        
        print()
        print("╔════════════════════════════════════════════════════════════════╗")
        print("║                    CLEANUP COMPLETE!                           ║")
        print("╚════════════════════════════════════════════════════════════════╝")
        print()
        print("Next step:")
        print("  python api_etl\\run_cloud_etl_multi_month.py")
        print()
        
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Cleanup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[ERROR] Cleanup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

