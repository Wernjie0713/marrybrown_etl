"""
Test connection to TIMEdotcom cloud SQL Server
Tests basic connectivity and database access
"""
import pyodbc
import sys
from datetime import datetime

# Cloud SQL Server credentials
SERVER = "10.0.1.194,1433"  # Private IP (VPN required)
# SERVER = "211.25.163.117,1433"  # Public IP (alternative)
DATABASE = "MarryBrown_DW"
USERNAME = "etl_user"  # Change if you used different credentials
PASSWORD = "ETL@MarryBrown2025!"  # Change to your actual password

def test_basic_connection():
    """Test 1: Basic connection to SQL Server"""
    print("\n" + "="*60)
    print("TEST 1: Basic Connection Test")
    print("="*60)
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SERVER};"
            f"DATABASE=master;"  # Connect to master first
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        
        print(f"Attempting to connect to: {SERVER}")
        print(f"Using username: {USERNAME}")
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # Get SQL Server version
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        
        print("\n[OK] CONNECTION SUCCESSFUL!")
        print(f"\nSQL Server Version:")
        print(version[:200])  # First 200 chars
        
        cursor.close()
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print("\n[FAIL] CONNECTION FAILED!")
        print(f"\nError: {e}")
        print("\nTroubleshooting tips:")
        print("1. [CHECK] Verify VPN is connected")
        print("2. [CHECK] Check SQL Server service is running on cloud server")
        print("3. [CHECK] Verify TCP/IP is enabled (port 1433)")
        print("4. [CHECK] Check Windows Firewall rules")
        print("5. [CHECK] Verify username/password are correct")
        return False

def test_database_access():
    """Test 2: Access MarryBrown_DW database"""
    print("\n" + "="*60)
    print("TEST 2: Database Access Test")
    print("="*60)
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        
        print(f"Connecting to database: {DATABASE}")
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # Get database name
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        
        # Get database size
        cursor.execute("""
            SELECT 
                SUM(size * 8.0 / 1024) as SizeMB
            FROM sys.master_files
            WHERE database_id = DB_ID()
        """)
        size_mb = cursor.fetchone()[0]
        
        print("\n[OK] DATABASE ACCESS SUCCESSFUL!")
        print(f"\nConnected to: {db_name}")
        print(f"Database size: {size_mb:.2f} MB")
        
        cursor.close()
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print("\n[FAIL] DATABASE ACCESS FAILED!")
        print(f"\nError: {e}")
        print("\nPossible issues:")
        print("1. Database 'MarryBrown_DW' doesn't exist")
        print("2. User doesn't have permission to access database")
        print("3. Check user mapping in SSMS")
        return False

def test_table_operations():
    """Test 3: Create and query test table"""
    print("\n" + "="*60)
    print("TEST 3: Table Operations Test")
    print("="*60)
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # Drop test table if exists
        print("\nCleaning up any existing test table...")
        cursor.execute("""
            IF OBJECT_ID('dbo.test_connection', 'U') IS NOT NULL
                DROP TABLE dbo.test_connection
        """)
        conn.commit()
        
        # Create test table
        print("Creating test table...")
        cursor.execute("""
            CREATE TABLE dbo.test_connection (
                id INT PRIMARY KEY,
                test_message VARCHAR(100),
                test_timestamp DATETIME
            )
        """)
        conn.commit()
        
        # Insert test data
        print("Inserting test data...")
        test_time = datetime.now()
        cursor.execute("""
            INSERT INTO dbo.test_connection (id, test_message, test_timestamp)
            VALUES (?, ?, ?)
        """, (1, "Connection test successful!", test_time))
        conn.commit()
        
        # Query test data
        print("Querying test data...")
        cursor.execute("SELECT * FROM dbo.test_connection")
        row = cursor.fetchone()
        
        print("\n[OK] TABLE OPERATIONS SUCCESSFUL!")
        print(f"\nTest data retrieved:")
        print(f"  ID: {row[0]}")
        print(f"  Message: {row[1]}")
        print(f"  Timestamp: {row[2]}")
        
        # Clean up
        print("\nCleaning up test table...")
        cursor.execute("DROP TABLE dbo.test_connection")
        conn.commit()
        
        cursor.close()
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print("\n[FAIL] TABLE OPERATIONS FAILED!")
        print(f"\nError: {e}")
        print("\nPossible issues:")
        print("1. User doesn't have CREATE TABLE permission")
        print("2. User doesn't have INSERT/SELECT permission")
        print("3. Check if user is in db_owner role")
        return False

def test_existing_tables():
    """Test 4: Check for existing tables"""
    print("\n" + "="*60)
    print("TEST 4: Existing Tables Check")
    print("="*60)
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # List all user tables
        cursor.execute("""
            SELECT 
                t.name as TableName,
                COUNT(c.column_id) as ColumnCount
            FROM sys.tables t
            LEFT JOIN sys.columns c ON t.object_id = c.object_id
            WHERE t.type = 'U'
            GROUP BY t.name
            ORDER BY t.name
        """)
        
        tables = cursor.fetchall()
        
        if len(tables) == 0:
            print("\n[WARNING] No tables found in database")
            print("\nThis is normal for a fresh installation.")
            print("Next step: Deploy dimension and fact tables")
        else:
            print(f"\n[OK] Found {len(tables)} tables in database:")
            print("\nTable Name                    | Column Count")
            print("-" * 50)
            for table in tables:
                print(f"{table[0]:<30} | {table[1]:>3}")
        
        cursor.close()
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print("\n[FAIL] TABLE CHECK FAILED!")
        print(f"\nError: {e}")
        return False

def main():
    """Run all connection tests"""
    print("\n" + "="*60)
    print("CLOUD SQL SERVER CONNECTION TEST SUITE")
    print("="*60)
    print(f"\nTarget Server: {SERVER}")
    print(f"Target Database: {DATABASE}")
    print(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")    
    print("\nWARNING: Make sure VPN is connected before running tests!")
    
    results = {
        "Basic Connection": test_basic_connection(),
        "Database Access": test_database_access(),
        "Table Operations": test_table_operations(),
        "Existing Tables": test_existing_tables()
    }
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "[PASS]" if result else "[FAIL]"
        print(f"{test_name:<25} {status}")
    
    print("\n" + "="*60)
    print(f"Result: {passed}/{total} tests passed")
    print("="*60)
    
    if passed == total:
        print("\n[SUCCESS] ALL TESTS PASSED!")
        print("\n[OK] Your cloud database is ready for deployment!")
        print("\nNext steps:")
        print("1. Deploy dimension tables (etl_dim_*.py)")
        print("2. Create fact table (transform_sales_facts.sql)")
        print("3. Deploy ETL scripts to cloud server")
    else:
        print("\n[WARNING] Some tests failed. Please review errors above.")
        print("\nRefer to the troubleshooting guide:")
        print("Notion: SQL Server Cloud Setup Guide - Step 6")
    
    return passed == total

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[WARNING] Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[FAIL] Unexpected error: {e}")
        sys.exit(1)

