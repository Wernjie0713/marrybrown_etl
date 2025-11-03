"""
Check Cloud ETL Status - Verify Multi-Month ETL Completion

Checks what data exists in cloud warehouse to determine if ETL completed.
Analyzes staging and fact tables for date ranges, counts, and data quality.

Author: YONG WERN JIE
Date: October 31, 2025
"""

import pyodbc
from dotenv import load_dotenv
import os
from datetime import datetime
from urllib.parse import quote_plus

# Load cloud environment
load_dotenv('.env.cloud')

def get_connection():
    """Create direct pyodbc connection to cloud warehouse"""
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


def check_staging_tables(cursor):
    """Check staging tables for data"""
    print("="*80)
    print("STAGING TABLES STATUS")
    print("="*80)
    print()
    
    # staging_sales
    cursor.execute("""
        SELECT 
            COUNT(*) AS TotalSales,
            COUNT(DISTINCT SaleID) AS UniqueSales,
            MIN(BusinessDateTime) AS EarliestDate,
            MAX(BusinessDateTime) AS LatestDate,
            SUM(CAST(GrandTotal AS FLOAT)) AS TotalRevenue
        FROM dbo.staging_sales
    """)
    row = cursor.fetchone()
    
    print("[STAGING_SALES]")
    print(f"   Total Rows: {row.TotalSales:,}")
    print(f"   Unique Sales: {row.UniqueSales:,}")
    if row.EarliestDate and row.LatestDate:
        print(f"   Date Range: {row.EarliestDate} to {row.LatestDate}")
        # Calculate month span
        earliest = row.EarliestDate
        latest = row.LatestDate
        months_span = (latest.year - earliest.year) * 12 + (latest.month - earliest.month) + 1
        print(f"   Month Span: {months_span} months")
    print(f"   Total Revenue: RM {row.TotalRevenue:,.2f}" if row.TotalRevenue else "   Total Revenue: RM 0.00")
    print()
    
    # staging_sales_items
    cursor.execute("SELECT COUNT(*) AS TotalItems FROM dbo.staging_sales_items")
    row = cursor.fetchone()
    print(f"[STAGING_SALES_ITEMS] {row.TotalItems:,} rows")
    print()
    
    # staging_payments
    cursor.execute("""
        SELECT 
            COUNT(*) AS TotalPayments,
            SUM(CAST(Amount AS FLOAT)) AS TotalAmount
        FROM dbo.staging_payments
    """)
    row = cursor.fetchone()
    print(f"[STAGING_PAYMENTS] {row.TotalPayments:,} rows")
    print(f"   Total Amount: RM {row.TotalAmount:,.2f}" if row.TotalAmount else "   Total Amount: RM 0.00")
    print()


def check_fact_table(cursor):
    """Check fact table for data"""
    print("="*80)
    print("FACT TABLE STATUS")
    print("="*80)
    print()
    
    cursor.execute("""
        SELECT 
            COUNT(*) AS TotalRows,
            COUNT(DISTINCT TransactionKey) AS UniqueSales,
            MIN(DateKey) AS EarliestDateKey,
            MAX(DateKey) AS LatestDateKey,
            SUM(TotalAmount) AS TotalRevenue
        FROM dbo.fact_sales_transactions
    """)
    row = cursor.fetchone()
    
    print("[FACT_SALES_TRANSACTIONS]")
    print(f"   Total Rows: {row.TotalRows:,}")
    print(f"   Unique Sales: {row.UniqueSales:,}")
    
    if row.EarliestDateKey and row.LatestDateKey:
        # Convert DateKey to readable date
        earliest_str = str(row.EarliestDateKey)
        latest_str = str(row.LatestDateKey)
        print(f"   Date Range: {earliest_str} to {latest_str}")
        
        # Parse year and month
        earliest_year = int(earliest_str[:4])
        earliest_month = int(earliest_str[4:6])
        latest_year = int(latest_str[:4])
        latest_month = int(latest_str[4:6])
        
        months_span = (latest_year - earliest_year) * 12 + (latest_month - earliest_month) + 1
        print(f"   Month Span: {months_span} months")
    
    print(f"   Total Revenue: RM {row.TotalRevenue:,.2f}" if row.TotalRevenue else "   Total Revenue: RM 0.00")
    print()


def check_monthly_breakdown(cursor):
    """Show breakdown by month"""
    print("="*80)
    print("MONTHLY BREAKDOWN")
    print("="*80)
    print()
    
    cursor.execute("""
        SELECT 
            LEFT(CAST(DateKey AS VARCHAR), 6) AS YearMonth,
            COUNT(*) AS TransactionCount,
            COUNT(DISTINCT TransactionKey) AS UniqueSales,
            SUM(TotalAmount) AS MonthlyRevenue
        FROM dbo.fact_sales_transactions
        GROUP BY LEFT(CAST(DateKey AS VARCHAR), 6)
        ORDER BY YearMonth
    """)
    
    rows = cursor.fetchall()
    
    if rows:
        print(f"{'Month':<12} {'Transactions':<15} {'Unique Sales':<15} {'Revenue':<15}")
        print("-" * 60)
        
        total_transactions = 0
        total_sales = 0
        total_revenue = 0
        
        for row in rows:
            year_month = row.YearMonth
            year = year_month[:4]
            month = year_month[4:6]
            month_str = f"{year}-{month}"
            
            total_transactions += row.TransactionCount
            total_sales += row.UniqueSales
            total_revenue += row.MonthlyRevenue if row.MonthlyRevenue else 0
            
            print(f"{month_str:<12} {row.TransactionCount:<15,} {row.UniqueSales:<15,} RM {row.MonthlyRevenue:>12,.2f}" if row.MonthlyRevenue else f"{month_str:<12} {row.TransactionCount:<15,} {row.UniqueSales:<15,} RM {0:>12,.2f}")
        
        print("-" * 60)
        print(f"{'TOTAL':<12} {total_transactions:<15,} {total_sales:<15,} RM {total_revenue:>12,.2f}")
        print()
        print(f"[OK] Data found for {len(rows)} months")
    else:
        print("[ERROR] No data in fact table")
    
    print()


def check_expected_months():
    """Show expected months (Oct 2018 - Dec 2019)"""
    print("="*80)
    print("EXPECTED ETL COVERAGE")
    print("="*80)
    print()
    print("[TARGET] October 2018 - December 2019")
    print("[EXPECTED] 15 months")
    print()
    print("Expected months:")
    expected = [
        "2018-10", "2018-11", "2018-12",
        "2019-01", "2019-02", "2019-03", "2019-04", "2019-05", "2019-06",
        "2019-07", "2019-08", "2019-09", "2019-10", "2019-11", "2019-12"
    ]
    for i, month in enumerate(expected, 1):
        print(f"  {i:2d}. {month}")
    print()


def check_data_quality(cursor):
    """Check for data quality issues"""
    print("="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    print()
    
    # Check for NULLs in critical fields
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN TransactionKey IS NULL THEN 1 ELSE 0 END) AS NullTransactionKey,
            SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS NullDateKey,
            SUM(CASE WHEN TimeKey IS NULL THEN 1 ELSE 0 END) AS NullTimeKey,
            SUM(CASE WHEN LocationKey IS NULL THEN 1 ELSE 0 END) AS NullLocationKey,
            SUM(CASE WHEN TotalAmount IS NULL THEN 1 ELSE 0 END) AS NullTotalAmount
        FROM dbo.fact_sales_transactions
    """)
    row = cursor.fetchone()
    
    issues = 0
    if row.NullTransactionKey > 0:
        print(f"[WARNING] NULL TransactionKeys: {row.NullTransactionKey}")
        issues += 1
    if row.NullDateKey > 0:
        print(f"[WARNING] NULL DateKeys: {row.NullDateKey}")
        issues += 1
    if row.NullTimeKey > 0:
        print(f"[WARNING] NULL TimeKeys: {row.NullTimeKey}")
        issues += 1
    if row.NullLocationKey > 0:
        print(f"[WARNING] NULL LocationKeys: {row.NullLocationKey}")
        issues += 1
    if row.NullTotalAmount > 0:
        print(f"[WARNING] NULL TotalAmounts: {row.NullTotalAmount}")
        issues += 1
    
    if issues == 0:
        print("[OK] No data quality issues detected")
    
    print()


def main():
    """Main execution"""
    print()
    print("=" * 80)
    print(" " * 20 + "CLOUD ETL STATUS CHECKER")
    print("=" * 80)
    print()
    
    start_time = datetime.now()
    print(f"Checking cloud warehouse at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        print("[OK] Connected to cloud warehouse")
        print()
        
        # Show expected coverage
        check_expected_months()
        
        # Check staging tables
        check_staging_tables(cursor)
        
        # Check fact table
        check_fact_table(cursor)
        
        # Monthly breakdown
        check_monthly_breakdown(cursor)
        
        # Data quality
        check_data_quality(cursor)
        
        cursor.close()
        conn.close()
        
        print("="*80)
        print("STATUS CHECK COMPLETE")
        print("="*80)
        print()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Check completed in {duration:.1f} seconds")
        print()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

