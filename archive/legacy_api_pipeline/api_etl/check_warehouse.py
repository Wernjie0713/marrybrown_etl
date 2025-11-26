"""
Quick script to check what data was loaded to the warehouse
"""

import os
import pyodbc
from dotenv import load_dotenv

load_dotenv('.env.cloud')

def check_warehouse():
    """Check staging tables and fact table for loaded data"""
    
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
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print("="*80)
    print("WAREHOUSE DATA CHECK")
    print("="*80)
    print()
    
    # Check staging tables
    print("[STAGING TABLES]")
    print()
    
    cursor.execute("SELECT COUNT(*) FROM dbo.staging_sales")
    staging_sales = cursor.fetchone()[0]
    print(f"  staging_sales: {staging_sales:,} rows")
    
    cursor.execute("SELECT COUNT(*) FROM dbo.staging_sales_items")
    staging_items = cursor.fetchone()[0]
    print(f"  staging_sales_items: {staging_items:,} rows")
    
    cursor.execute("SELECT COUNT(*) FROM dbo.staging_payments")
    staging_payments = cursor.fetchone()[0]
    print(f"  staging_payments: {staging_payments:,} rows")
    
    print()
    print("[FACT TABLE]")
    print()
    
    cursor.execute("SELECT COUNT(*) FROM dbo.fact_sales_transactions")
    fact_rows = cursor.fetchone()[0]
    print(f"  fact_sales_transactions: {fact_rows:,} rows")
    
    if fact_rows > 0:
        cursor.execute("""
            SELECT 
                MIN(DateKey) as MinDate,
                MAX(DateKey) as MaxDate,
                COUNT(DISTINCT DateKey) as UniqueDates,
                COUNT(DISTINCT SaleNumber) as UniqueSales,
                SUM(TotalAmount) as TotalAmount
            FROM dbo.fact_sales_transactions
        """)
        row = cursor.fetchone()
        print(f"  Date Range: {row.MinDate} to {row.MaxDate}")
        print(f"  Unique Dates: {row.UniqueDates}")
        print(f"  Unique Sales: {row.UniqueSales:,}")
        print(f"  Total Amount: RM {row.TotalAmount:,.2f}")
        
        print()
        print("[MONTHLY BREAKDOWN]")
        print()
        cursor.execute("""
            SELECT 
                LEFT(CAST(DateKey AS VARCHAR), 6) as YearMonth,
                COUNT(*) as FactRows,
                COUNT(DISTINCT SaleNumber) as UniqueSales,
                SUM(TotalAmount) as TotalAmount
            FROM dbo.fact_sales_transactions
            GROUP BY LEFT(CAST(DateKey AS VARCHAR), 6)
            ORDER BY YearMonth
        """)
        
        for row in cursor.fetchall():
            year_month = f"{row.YearMonth[:4]}-{row.YearMonth[4:]}"
            print(f"  {year_month}: {row.FactRows:>8,} rows | {row.UniqueSales:>6,} sales | RM {row.TotalAmount:>12,.2f}")
    
    print()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_warehouse()

