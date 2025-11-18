"""
Test Local Database Connection (FakeRestaurantDB)
Quick script to verify .env.local configuration before running API ETL

Author: YONG WERN JIE
Date: October 28, 2025
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load .env.local
load_dotenv('.env.local')

print("="*80)
print("LOCAL DATABASE CONNECTION TEST")
print("="*80)
print()

# Read configuration
driver = os.getenv("TARGET_DRIVER", "ODBC Driver 17 for SQL Server")
server = os.getenv("TARGET_SERVER", "localhost")
database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
user = os.getenv("TARGET_USERNAME", "sa")
password = os.getenv("TARGET_PASSWORD", "")

print("Configuration from .env.local:")
print(f"  Driver: {driver}")
print(f"  Server: {server}")
print(f"  Database: {database}")
print(f"  Username: {user}")
print(f"  Password: {'*' * len(password) if password else '(empty)'}")
print()

# Test connection
print("Testing connection...")
print()

try:
    # Build connection string
    driver_formatted = driver.replace(" ", "+")
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver_formatted}"
        "&TrustServerCertificate=yes"
    )
    
    # Create engine
    engine = create_engine(connection_uri, pool_pre_ping=True)
    
    # Test connection
    with engine.connect() as conn:
        # Test basic query
        result = conn.execute(text("SELECT @@VERSION as Version, DB_NAME() as CurrentDB"))
        row = result.fetchone()
        
        print("✅ CONNECTION SUCCESSFUL!")
        print()
        print(f"Connected to: {row.CurrentDB}")
        print(f"SQL Server Version: {row.Version[:50]}...")
        print()
        
        # Check for required tables
        print("Checking required tables...")
        
        tables_to_check = [
            'fact_sales_transactions',
            'fact_sales_transactions_api',
            'staging_sales_api',
            'staging_sales_items_api',
            'staging_payments_api',
            'dim_date',
            'dim_locations',
            'dim_products',
            'dim_staff',
            'dim_payment_types'
        ]
        
        for table_name in tables_to_check:
            check_query = text("""
                SELECT COUNT(*) as table_exists
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = :table_name
                  AND TABLE_SCHEMA = 'dbo'
            """)
            result = conn.execute(check_query, {"table_name": table_name})
            exists = result.fetchone()[0]
            
            if exists:
                # Get row count
                count_query = text(f"SELECT COUNT(*) as row_count FROM dbo.{table_name}")
                count_result = conn.execute(count_query)
                row_count = count_result.fetchone()[0]
                print(f"  ✅ {table_name} - {row_count:,} rows")
            else:
                if table_name in ['fact_sales_transactions_api', 'staging_sales_api', 
                                 'staging_sales_items_api', 'staging_payments_api']:
                    print(f"  ⚠️  {table_name} - NOT FOUND (will be created by schema script)")
                else:
                    print(f"  ❌ {table_name} - NOT FOUND (REQUIRED!)")
        
        print()
        print("="*80)
        print("CONNECTION TEST COMPLETE!")
        print("="*80)
        print()
        
        # Summary
        missing_required = []
        for table in ['fact_sales_transactions', 'dim_date', 'dim_locations', 
                     'dim_products', 'dim_staff', 'dim_payment_types']:
            check_query = text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = :table_name AND TABLE_SCHEMA = 'dbo'
            """)
            result = conn.execute(check_query, {"table_name": table})
            if result.fetchone()[0] == 0:
                missing_required.append(table)
        
        if not missing_required:
            print("✅ All required tables exist!")
            print()
            print("Next steps:")
            print("  1. Run: sqlcmd -S localhost -d FakeRestaurantDB -E -i create_fact_table_api.sql")
            print("  2. Run: python api_etl\\run_api_etl_oct2018.py")
        else:
            print("❌ Missing required tables:")
            for table in missing_required:
                print(f"    - {table}")
            print()
            print("Action required:")
            print("  Run dimension ETL scripts first to create missing tables")

except Exception as e:
    print("❌ CONNECTION FAILED!")
    print()
    print(f"Error: {e}")
    print()
    print("Common fixes:")
    print("  1. Check .env.local exists and has correct credentials")
    print("  2. Verify SQL Server is running")
    print("  3. Check if database 'FakeRestaurantDB' exists")
    print("  4. Try using Windows Authentication (leave username/password empty)")
    print()
    print("To use Windows Authentication, update .env.local:")
    print("  TARGET_USERNAME=")
    print("  TARGET_PASSWORD=")
    print()
    
    import traceback
    traceback.print_exc()

