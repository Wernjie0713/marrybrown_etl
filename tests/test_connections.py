import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

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
    xilnex_driver = os.getenv("XILNEX_DRIVER", "")
    if not xilnex_driver:
        raise ValueError("XILNEX_DRIVER not found in environment variables")
    
    xilnex_connection_uri = (
        "mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        .format(
            user=os.getenv("XILNEX_USERNAME"),
            password=os.getenv("XILNEX_PASSWORD"),
            server=os.getenv("XILNEX_SERVER"),
            database=os.getenv("XILNEX_DATABASE"),
            driver=xilnex_driver.replace(" ", "+"),
        )
    )
    source_engine = create_engine(xilnex_connection_uri)
    with source_engine.connect() as connection:
        print("✅ Successfully connected to Xilnex source database!")
        print(f"   Server: {os.getenv('XILNEX_SERVER')}")
        print(f"   Database: {os.getenv('XILNEX_DATABASE')}\n")

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

