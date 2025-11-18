"""
Quick database status check
"""
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

# Connect to warehouse
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('WAREHOUSE_SERVER')};"
    f"DATABASE={os.getenv('WAREHOUSE_DATABASE')};"
    f"UID={os.getenv('WAREHOUSE_USER')};"
    f"PWD={os.getenv('WAREHOUSE_PASSWORD')}"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("=" * 80)
print("DATABASE STATUS CHECK")
print("=" * 80)
print()

# Check staging tables
print("[STAGING TABLES]")
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

# Check fact table
print("[FACT TABLE]")
cursor.execute("SELECT COUNT(*) FROM dbo.fact_sales_transactions")
fact_count = cursor.fetchone()[0]
print(f"  fact_sales_transactions: {fact_count:,} rows")

if fact_count > 0:
    print()
    print("[FACT TABLE DETAILS]")
    cursor.execute("""
        SELECT 
            MIN(DateKey) as MinDate,
            MAX(DateKey) as MaxDate,
            COUNT(DISTINCT DateKey) as DistinctDates,
            COUNT(DISTINCT YEAR(CAST(CAST(DateKey AS VARCHAR) AS DATE)) * 100 + MONTH(CAST(CAST(DateKey AS VARCHAR) AS DATE))) as DistinctMonths
        FROM dbo.fact_sales_transactions
    """)
    row = cursor.fetchone()
    print(f"  Date Range: {row[0]} to {row[1]}")
    print(f"  Distinct Dates: {row[2]}")
    print(f"  Distinct Months: {row[3]}")
    
    print()
    print("[MONTHS LOADED]")
    cursor.execute("""
        SELECT 
            YEAR(CAST(CAST(DateKey AS VARCHAR) AS DATE)) as Year,
            MONTH(CAST(CAST(DateKey AS VARCHAR) AS DATE)) as Month,
            COUNT(*) as RowCount,
            MIN(CAST(CAST(DateKey AS VARCHAR) AS DATE)) as FirstDate,
            MAX(CAST(CAST(DateKey AS VARCHAR) AS DATE)) as LastDate
        FROM dbo.fact_sales_transactions
        GROUP BY 
            YEAR(CAST(CAST(DateKey AS VARCHAR) AS DATE)),
            MONTH(CAST(CAST(DateKey AS VARCHAR) AS DATE))
        ORDER BY Year, Month
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}-{row[1]:02d}: {row[2]:,} rows (from {row[3]} to {row[4]})")

cursor.close()
conn.close()

print()
print("=" * 80)

