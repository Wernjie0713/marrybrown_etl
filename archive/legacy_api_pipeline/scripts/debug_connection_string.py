"""
Debug script to verify .env.cloud connection string
"""
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv('.env.cloud')

driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
server = os.getenv("TARGET_SERVER")
database = os.getenv("TARGET_DATABASE")
user = os.getenv("TARGET_USERNAME")
password = os.getenv("TARGET_PASSWORD")
encoded_password = quote_plus(password)

connection_uri_raw = (
    f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    "&TrustServerCertificate=yes&timeout=30"
)

connection_uri_encoded = (
    f"mssql+pyodbc://{user}:{encoded_password}@{server}/{database}?driver={driver}"
    "&TrustServerCertificate=yes&timeout=30"
)

print("="*80)
print("CONNECTION STRING DEBUG")
print("="*80)
print(f"Driver (raw): {os.getenv('TARGET_DRIVER')}")
print(f"Driver (encoded): {driver}")
print(f"Server: {server}")
print(f"Database: {database}")
print(f"Username: {user}")
print(f"Password (raw): {password}")
print(f"Password (URL-encoded): {encoded_password}")
print()
print("❌ WRONG - Raw password (causes parsing error):")
print(connection_uri_raw)
print()
print("✅ CORRECT - URL-encoded password:")
print(connection_uri_encoded)
print("="*80)

