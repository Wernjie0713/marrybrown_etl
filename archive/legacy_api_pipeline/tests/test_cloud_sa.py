"""
Test connection with sa account
"""
import pyodbc

SERVER = "10.0.1.194,1433"
DATABASE = "MarryBrown_DW"
USERNAME = "sa"
PASSWORD = input("Enter sa password: ")  # The password you set during SQL Server installation

try:
    print(f"\nConnecting to {SERVER} as '{USERNAME}'...")
    
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes;",
        timeout=10
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT DB_NAME(), SUSER_NAME()")
    row = cursor.fetchone()
    
    print(f"\n[OK] CONNECTION SUCCESSFUL!")
    print(f"Connected to: {row[0]}")
    print(f"Logged in as: {row[1]}")
    
    # Check if etl_user exists
    cursor.execute("""
        SELECT name, type_desc, create_date 
        FROM sys.server_principals 
        WHERE name = 'etl_user'
    """)
    
    user_info = cursor.fetchone()
    if user_info:
        print(f"\n[OK] Login 'etl_user' exists on server")
        print(f"  Type: {user_info[1]}")
        print(f"  Created: {user_info[2]}")
    else:
        print("\n[FAIL] Login 'etl_user' NOT found on server!")
    
    # Check database user
    cursor.execute("""
        SELECT name, type_desc, create_date 
        FROM sys.database_principals 
        WHERE name = 'etl_user'
    """)
    
    db_user_info = cursor.fetchone()
    if db_user_info:
        print(f"\n[OK] User 'etl_user' exists in database")
        print(f"  Type: {db_user_info[1]}")
        print(f"  Created: {db_user_info[2]}")
        
        # Check roles
        cursor.execute("""
            SELECT r.name
            FROM sys.database_role_members rm
            JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
            JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id
            WHERE u.name = 'etl_user'
        """)
        
        roles = [row[0] for row in cursor.fetchall()]
        print(f"  Roles: {', '.join(roles) if roles else 'None'}")
    else:
        print("\n[FAIL] User 'etl_user' NOT found in database!")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\n[FAIL] Error: {e}")

