"""
Quick connection test to cloud SQL Server
Run this first to verify basic connectivity
"""
import pyodbc

# CHANGE THESE TO YOUR ACTUAL CREDENTIALS
SERVER = "10.0.1.194,1433"  # Private IP (VPN required)
# SERVER = "211.25.163.117,1433"  # Public IP (alternative)
DATABASE = "MarryBrown_DW"
USERNAME = "etl_user"
PASSWORD = "ETL@MarryBrown2025!"  # ⚠️ CHANGE THIS!

def quick_test():
    print("="*60)
    print("Quick Cloud SQL Server Connection Test")
    print("="*60)
    print(f"\nServer: {SERVER}")
    print(f"Database: {DATABASE}")
    print(f"Username: {USERNAME}")
    print("\nWARNING: Make sure VPN is connected!\n")
    
    try:
        print("Connecting...")
        
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
        cursor.execute("SELECT DB_NAME(), @@VERSION")
        row = cursor.fetchone()
        
        print("[OK] CONNECTION SUCCESSFUL!\n")
        print(f"Connected to database: {row[0]}")
        print(f"SQL Server version: {row[1][:80]}...")
        
        cursor.close()
        conn.close()
        
        print("\n[SUCCESS] Your cloud database is accessible!")
        print("\nNext step: Run 'python test_cloud_connection.py' for full tests")
        
        return True
        
    except pyodbc.Error as e:
        print("[FAIL] CONNECTION FAILED!\n")
        print(f"Error: {e}\n")
        print("Troubleshooting:")
        print("1. Is VPN connected? (Check OpenVPN)")
        print("2. Is SQL Server running on cloud server?")
        print("3. Is the password correct?")
        print("4. Did you enable TCP/IP and configure firewall?")
        
        return False

if __name__ == "__main__":
    quick_test()

