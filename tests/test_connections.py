import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path and load .env.local
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load the credentials from .env.local file
env_path = PROJECT_ROOT / ".env.local"
if not env_path.exists():
    print(f"⚠️  Warning: .env.local not found at {env_path}")
    print("   Falling back to .env file...")
    env_path = PROJECT_ROOT / ".env"

load_dotenv(env_path)
print(f"📁 Loading environment from: {env_path}")

print("\nAttempting to connect to databases...\n")

# --- 1. CONFIGURE AND TEST SOURCE (XILNEX) CONNECTION ---
try:
    import pyodbc
    xilnex_driver = os.getenv("XILNEX_DRIVER", "")
    if not xilnex_driver:
        raise ValueError("XILNEX_DRIVER not found in environment variables")
    
    # Build connection string with ApplicationIntent support
    app_intent = os.getenv("XILNEX_APPLICATION_INTENT", "")
    conn_str = (
        f"DRIVER={{{xilnex_driver}}};"
        f"SERVER={os.getenv('XILNEX_SERVER')};"
        f"DATABASE={os.getenv('XILNEX_DATABASE')};"
        f"UID={os.getenv('XILNEX_USERNAME')};"
        f"PWD={os.getenv('XILNEX_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    # Add ApplicationIntent if specified
    if app_intent:
        conn_str += f"ApplicationIntent={app_intent};"
    
    with pyodbc.connect(conn_str, timeout=30) as connection:
        cursor = connection.cursor()
        
        # Check if connected to replica (read-only secondary)
        cursor.execute("SELECT DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS Updateability")
        row = cursor.fetchone()
        updateability = row[0] if row else "UNKNOWN"
        
        is_replica = updateability == "READ_ONLY"
        replica_status = "🔄 REPLICA (Read-Only)" if is_replica else "⚡ PRIMARY (Read-Write)"
        
        print("✅ Successfully connected to Xilnex source database!")
        print(f"   Server: {os.getenv('XILNEX_SERVER')}")
        print(f"   Database: {os.getenv('XILNEX_DATABASE')}")
        print(f"   ApplicationIntent: {app_intent if app_intent else 'Not set (default)'}")
        print(f"   Connection Type: {replica_status}\n")

except Exception as e:
    print(f"❌ FAILED to connect to Xilnex source database: {e}\n")


# --- 2. CONFIGURE AND TEST TARGET (LOCAL WAREHOUSE) CONNECTION ---
try:
    import pyodbc
    target_driver = os.getenv("TARGET_DRIVER", "")
    if not target_driver:
        raise ValueError("TARGET_DRIVER not found in environment variables")
    
    # Build connection string manually for pyodbc to support "tcp:" prefix
    # which SQLAlchemy's create_url/create_engine often chokes on.
    conn_str = (
        f"DRIVER={{{target_driver}}};"
        f"SERVER={os.getenv('TARGET_SERVER')};"
        f"DATABASE={os.getenv('TARGET_DATABASE')};"
        f"UID={os.getenv('TARGET_USERNAME')};"
        f"PWD={os.getenv('TARGET_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )
    
    print(f"   Connecting to: {os.getenv('TARGET_SERVER')}...")
    with pyodbc.connect(conn_str, timeout=10) as connection:
        print("✅ Successfully connected to target database! (via pyodbc)")
        print(f"   Server: {os.getenv('TARGET_SERVER')}")
        print(f"   Database: {os.getenv('TARGET_DATABASE')}\n")

except Exception as e:
    print(f"❌ FAILED to connect to target database: {e}\n")

print("Connection test complete.")

