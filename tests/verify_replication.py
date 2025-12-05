"""
Verify ALL table replication by checking row counts in cloud warehouse.
Includes both sales tables (date-filtered) and reference tables.
"""

import sys
from pathlib import Path

import pyodbc

# Add project root to import config
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config

# Cloud warehouse connection
conn_str = config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)

SCHEMA_PREFIX = "dbo.com_5013_"

# Sales tables (date-filtered)
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

# Reference tables (full table refresh)
REFERENCE_TABLES = [
    "APP_4_ITEM",
    "APP_4_STOCK",
    "APP_4_CUSTOMER",
    "APP_4_POINTRECORD",
    "LOCATION_DETAIL",
    "APP_4_VOUCHER_MASTER",
    "APP_4_CASHIER_DRAWER",
    "APP_4_SALESDELIVERY",
    "APP_4_EXTENDEDSALESITEM",
]


def check_table_counts():
    print("=" * 65)
    print("Cloud Warehouse - Complete Data Verification")
    print("=" * 65)
    print(f"Server: {config.TARGET_SQL_CONFIG['server']}")
    print(f"Database: {config.TARGET_SQL_CONFIG['database']}")
    print("=" * 65)
    
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        print("\n✅ Connected to cloud warehouse!\n")
        
        # --- SALES TABLES ---
        print("📊 SALES TABLES (Date-Filtered)")
        print("-" * 65)
        print(f"{'Table Name':<40} {'Row Count':>15}")
        print("-" * 65)
        
        sales_total = 0
        for table in SALES_TABLES:
            try:
                full_table_name = f"{SCHEMA_PREFIX}{table}"
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                count = cursor.fetchone()[0]
                sales_total += count
                status = "✓" if count > 0 else "○"
                print(f"{status} {table:<38} {count:>15,}")
            except Exception as e:
                print(f"✗ {table:<38} ERROR: {e}")
        
        print("-" * 65)
        print(f"{'Sales Subtotal':>41} {sales_total:>15,}")
        print()
        
        # --- REFERENCE TABLES ---
        print("📚 REFERENCE TABLES (Full Refresh)")
        print("-" * 65)
        print(f"{'Table Name':<40} {'Row Count':>15}")
        print("-" * 65)
        
        ref_total = 0
        for table in REFERENCE_TABLES:
            try:
                full_table_name = f"{SCHEMA_PREFIX}{table}"
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                count = cursor.fetchone()[0]
                ref_total += count
                status = "✓" if count > 0 else "○"
                print(f"{status} {table:<38} {count:>15,}")
            except Exception as e:
                print(f"✗ {table:<38} ERROR: {e}")
        
        print("-" * 65)
        print(f"{'Reference Subtotal':>41} {ref_total:>15,}")
        print()
        
        # --- GRAND TOTAL ---
        print("=" * 65)
        print(f"{'GRAND TOTAL':>41} {sales_total + ref_total:>15,}")
        print("=" * 65)
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")


if __name__ == "__main__":
    check_table_counts()
