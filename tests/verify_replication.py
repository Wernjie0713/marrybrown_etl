"""
Quick script to verify data replication by checking row counts in sales tables.
"""

import pyodbc

# Cloud warehouse connection credentials
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=10.0.1.194,1433;"
    "DATABASE=MarryBrown_DW;"
    "UID=etl_user;"
    "PWD=ETL@MarryBrown2025!;"
    "TrustServerCertificate=yes;"
)

# Sales tables with date columns (from DATE_FILTER_COLUMNS)
# Using dbo.com_5013_ prefix for cloud warehouse (underscore, not dot)
SCHEMA_PREFIX = "dbo.com_5013_"

SALES_TABLES = [
    "APP_4_SALES",
    "APP_4_SALESITEM",
    "APP_4_PAYMENT",
    "APP_4_VOIDSALESITEM",
    "APP_4_SALESCREDITNOTE",
    "APP_4_SALESCREDITNOTEITEM",
    "APP_4_SALESDEBITNOTE",
    "APP_4_SALESDEBITNOTEITEM",
    "APP_4_EPAYMENTLOG",
    "APP_4_VOUCHER",
]

def check_table_counts():
    print("=" * 60)
    print("Cloud Warehouse Data Verification")
    print("=" * 60)
    print(f"Server: 10.0.1.194,1433")
    print(f"Database: MarryBrown_DW")
    print("=" * 60)
    
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        print("\n✅ Connected to cloud warehouse!\n")
        
        print(f"{'Table Name':<35} {'Row Count':>15}")
        print("-" * 52)
        
        total_rows = 0
        for table in SALES_TABLES:
            try:
                full_table_name = f"{SCHEMA_PREFIX}{table}"
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                count = cursor.fetchone()[0]
                total_rows += count
                status = "✓" if count > 0 else "○"
                print(f"{status} {table:<33} {count:>15,}")
            except Exception as e:
                print(f"✗ {table:<33} ERROR: {e}")
        
        print("-" * 52)
        print(f"{'TOTAL':>36} {total_rows:>15,}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")

if __name__ == "__main__":
    check_table_counts()
