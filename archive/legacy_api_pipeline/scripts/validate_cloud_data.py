"""
Validate data in cloud warehouse after ETL
"""
import pyodbc
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

load_dotenv('.env.cloud')

def get_connection():
    """Get direct pyodbc connection to cloud warehouse"""
    driver = os.getenv('TARGET_DRIVER', 'ODBC Driver 18 for SQL Server')
    server = os.getenv('TARGET_SERVER', '10.0.1.194,1433')
    database = os.getenv('TARGET_DATABASE', 'MarryBrown_DW')
    username = os.getenv('TARGET_USERNAME', 'etl_user')
    password = os.getenv('TARGET_PASSWORD')
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )
    
    return pyodbc.connect(conn_str)

def run_validation():
    """Run comprehensive data validation"""
    print("="*80)
    print("CLOUD WAREHOUSE DATA VALIDATION")
    print("="*80)
    print()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # ========== 1. STAGING TABLES ROW COUNTS ==========
    print("[1/5] Staging Tables Row Counts")
    print("-" * 80)
    
    staging_queries = {
        'staging_sales': "SELECT COUNT(*) FROM dbo.staging_sales",
        'staging_sales_items': "SELECT COUNT(*) FROM dbo.staging_sales_items",
        'staging_payments': "SELECT COUNT(*) FROM dbo.staging_payments"
    }
    
    for table, query in staging_queries.items():
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print(f"  {table:25s}: {count:,}")
    print()
    
    # ========== 2. FACT TABLE ROW COUNTS ==========
    print("[2/5] Fact Table Row Counts")
    print("-" * 80)
    
    cursor.execute("SELECT COUNT(*) FROM dbo.fact_sales_transactions")
    fact_count = cursor.fetchone()[0]
    print(f"  fact_sales_transactions    : {fact_count:,}")
    print()
    
    # ========== 3. DATA QUALITY CHECKS ==========
    print("[3/5] Data Quality Checks")
    print("-" * 80)
    
    # Check for NULL critical fields in staging_sales
    cursor.execute("""
        SELECT 
            COUNT(*) AS TotalRows,
            SUM(CASE WHEN SaleID IS NULL THEN 1 ELSE 0 END) AS NullSaleID,
            SUM(CASE WHEN OutletID IS NULL THEN 1 ELSE 0 END) AS NullOutletID,
            SUM(CASE WHEN BusinessDateTime IS NULL THEN 1 ELSE 0 END) AS NullBusinessDateTime
        FROM dbo.staging_sales
    """)
    row = cursor.fetchone()
    print(f"  Staging Sales Quality:")
    print(f"    Total Rows: {row[0]:,}")
    print(f"    NULL SaleID: {row[1]:,}")
    print(f"    NULL OutletID: {row[2]:,}")
    print(f"    NULL BusinessDateTime: {row[3]:,}")
    print()
    
    # Check for NULL critical fields in fact table
    cursor.execute("""
        SELECT 
            COUNT(*) AS TotalRows,
            SUM(CASE WHEN TransactionKey IS NULL THEN 1 ELSE 0 END) AS NullTransactionKey,
            SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS NullDateKey,
            SUM(CASE WHEN TotalAmount IS NULL THEN 1 ELSE 0 END) AS NullTotalAmount
        FROM dbo.fact_sales_transactions
    """)
    row = cursor.fetchone()
    print(f"  Fact Table Quality:")
    print(f"    Total Rows: {row[0]:,}")
    print(f"    NULL TransactionKey: {row[1]:,}")
    print(f"    NULL DateKey: {row[2]:,}")
    print(f"    NULL TotalAmount: {row[3]:,}")
    print()
    
    # ========== 4. DATE RANGE VALIDATION ==========
    print("[4/5] Date Range Validation")
    print("-" * 80)
    
    cursor.execute("""
        SELECT 
            MIN(BusinessDateTime) AS MinDate,
            MAX(BusinessDateTime) AS MaxDate,
            COUNT(DISTINCT CAST(BusinessDateTime AS DATE)) AS UniqueDates
        FROM dbo.staging_sales
    """)
    row = cursor.fetchone()
    print(f"  Staging Sales:")
    print(f"    Min Date: {row[0]}")
    print(f"    Max Date: {row[1]}")
    print(f"    Unique Dates: {row[2]:,}")
    print()
    
    cursor.execute("""
        SELECT 
            MIN(DateKey) AS MinDateKey,
            MAX(DateKey) AS MaxDateKey,
            COUNT(DISTINCT DateKey) AS UniqueDates
        FROM dbo.fact_sales_transactions
    """)
    row = cursor.fetchone()
    print(f"  Fact Table:")
    print(f"    Min DateKey: {row[0]}")
    print(f"    Max DateKey: {row[1]}")
    print(f"    Unique DateKeys: {row[2]:,}")
    print()
    
    # ========== 5. SAMPLE DATA ==========
    print("[5/5] Sample Data (First 3 Records)")
    print("-" * 80)
    
    cursor.execute("""
        SELECT TOP 3
            SaleID,
            OutletID,
            BusinessDateTime,
            GrandTotal,
            Status
        FROM dbo.staging_sales
        ORDER BY BusinessDateTime
    """)
    
    print("  Staging Sales Sample:")
    for row in cursor.fetchall():
        print(f"    SaleID: {row[0][:20] if len(row[0]) > 20 else row[0]}...")
        print(f"    OutletID: {row[1]}")
        print(f"    BusinessDateTime: {row[2]}")
        print(f"    GrandTotal: RM {row[3]:,.2f}")
        print(f"    Status: {row[4]}")
        print()
    
    cursor.execute("""
        SELECT TOP 3
            TransactionKey,
            DateKey,
            TimeKey,
            LocationKey,
            TotalAmount
        FROM dbo.fact_sales_transactions
        ORDER BY DateKey, TimeKey
    """)
    
    print("  Fact Table Sample:")
    for row in cursor.fetchall():
        print(f"    TransactionKey: {row[0]}")
        print(f"    DateKey: {row[1]}")
        print(f"    TimeKey: {row[2]}")
        print(f"    LocationKey: {row[3]}")
        print(f"    TotalAmount: RM {row[4]:,.2f}")
        print()
    
    # ========== 6. AGGREGATION VALIDATION ==========
    print("[6/5] Aggregation Validation")
    print("-" * 80)
    
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT SaleID) AS UniqueSales,
            SUM(GrandTotal) AS TotalSales,
            AVG(GrandTotal) AS AvgSale,
            MIN(GrandTotal) AS MinSale,
            MAX(GrandTotal) AS MaxSale
        FROM dbo.staging_sales
    """)
    row = cursor.fetchone()
    print(f"  Staging Sales Summary:")
    print(f"    Unique Sales: {row[0]:,}")
    print(f"    Total Sales: RM {row[1]:,.2f}")
    print(f"    Avg Sale: RM {row[2]:,.2f}")
    print(f"    Min Sale: RM {row[3]:,.2f}")
    print(f"    Max Sale: RM {row[4]:,.2f}")
    print()
    
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT SaleNumber) AS UniqueSales,
            SUM(TotalAmount) AS TotalSales,
            COUNT(*) AS TotalRows
        FROM dbo.fact_sales_transactions
    """)
    row = cursor.fetchone()
    print(f"  Fact Table Summary:")
    print(f"    Unique Sales: {row[0]:,}")
    print(f"    Total Sales: RM {row[1]:,.2f}")
    print(f"    Total Rows: {row[2]:,}")
    print()
    
    # ========== FINAL STATUS ==========
    print("="*80)
    print("VALIDATION COMPLETE")
    print("="*80)
    print()
    
    # Check for issues
    issues = []
    
    if fact_count == 0:
        issues.append("CRITICAL: Fact table is empty!")
    
    cursor.execute("SELECT COUNT(*) FROM dbo.staging_sales WHERE SaleID IS NULL")
    null_ids = cursor.fetchone()[0]
    if null_ids > 0:
        issues.append(f"WARNING: {null_ids:,} sales with NULL IDs")
    
    if issues:
        print("[ISSUES FOUND]")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("[OK] All validation checks passed!")
    
    print()
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        run_validation()
    except Exception as e:
        print(f"\n[ERROR] Validation failed: {e}")
        import traceback
        traceback.print_exc()

