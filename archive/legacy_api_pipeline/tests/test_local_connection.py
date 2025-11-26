# test_connection.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

load_dotenv('.env.local')

driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
server = os.getenv("TARGET_SERVER", "localhost")
database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
user = os.getenv("TARGET_USERNAME", "etl_user")
password = quote_plus(os.getenv("TARGET_PASSWORD", ""))

connection_uri = (
    f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    "&TrustServerCertificate=yes"
    "&timeout=60"
    "&login_timeout=60"
)

try:
    engine = create_engine(connection_uri, pool_pre_ping=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT @@VERSION as version, USER_NAME() as db_user"))
        row = result.fetchone()
        print(f"[OK] Connection successful!")
        print(f"   User: {row.db_user}")
        print(f"   Server: {server}")
        print(f"   Database: {database}")
        
        # Test permissions
        conn.execute(text("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES"))
        print(f"   [OK] SELECT permission: OK")
        
except Exception as e:
    print(f"[ERROR] Connection failed: {e}")