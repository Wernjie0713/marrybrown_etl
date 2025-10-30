"""
Test connection using Windows Authentication (no password needed)
Run this ON THE CLOUD SERVER (via Remote Desktop)
"""
import pyodbc

SERVER = "localhost"  # or "10.0.1.194"
DATABASE = "MarryBrown_DW"

try:
    print("Connecting using Windows Authentication...")
    
    # Windows Authentication - no username/password needed!
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;",  # This uses Windows auth
        timeout=10
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT DB_NAME(), SUSER_NAME()")
    row = cursor.fetchone()
    
    print(f"\n[OK] CONNECTION SUCCESSFUL!")
    print(f"Connected to: {row[0]}")
    print(f"Logged in as: {row[1]}")
    
    # Check etl_user login
    print("\nChecking etl_user setup...")
    cursor.execute("""
        SELECT name, type_desc, is_disabled
        FROM sys.server_principals 
        WHERE name = 'etl_user'
    """)
    
    login = cursor.fetchone()
    if login:
        print(f"[OK] Login exists: {login[0]} ({login[1]})")
        print(f"     Disabled: {login[2]}")
    else:
        print("[FAIL] Login 'etl_user' not found!")
    
    # Check database user
    cursor.execute("""
        SELECT name, type_desc
        FROM sys.database_principals 
        WHERE name = 'etl_user'
    """)
    
    user = cursor.fetchone()
    if user:
        print(f"[OK] Database user exists: {user[0]} ({user[1]})")
    else:
        print("[FAIL] Database user 'etl_user' not found!")
    
    # Check role membership
    cursor.execute("""
        SELECT r.name
        FROM sys.database_role_members rm
        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id
        WHERE u.name = 'etl_user'
    """)
    
    roles = cursor.fetchall()
    if roles:
        print(f"[OK] Roles: {', '.join([r[0] for r in roles])}")
    else:
        print("[FAIL] No roles assigned!")
    
    # Try to test if we can actually login as etl_user
    print("\n" + "="*60)
    print("Testing etl_user login...")
    print("="*60)
    
    test_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID=etl_user;"
        f"PWD=ETL@MarryBrown2025!;"
        f"TrustServerCertificate=yes;",
        timeout=10
    )
    
    test_cursor = test_conn.cursor()
    test_cursor.execute("SELECT SUSER_NAME()")
    test_user = test_cursor.fetchone()[0]
    
    print(f"[OK] etl_user login SUCCESSFUL!")
    print(f"Logged in as: {test_user}")
    
    test_cursor.close()
    test_conn.close()
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\n[FAIL] Error: {e}")

