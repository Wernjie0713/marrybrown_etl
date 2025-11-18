import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load the credentials from the .env file
load_dotenv()

print("Attempting to connect to databases...")

# --- 1. CONFIGURE AND TEST SOURCE (XILNEX) CONNECTION ---
try:
    xilnex_connection_uri = (
        "mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        .format(
            user=os.getenv("XILNEX_USERNAME"),
            password=os.getenv("XILNEX_PASSWORD"),
            server=os.getenv("XILNEX_SERVER"),
            database=os.getenv("XILNEX_DATABASE"),
            driver=os.getenv("XILNEX_DRIVER").replace(" ", "+"),
        )
    )
    source_engine = create_engine(xilnex_connection_uri)
    with source_engine.connect() as connection:
        print("✅ Successfully connected to Xilnex source database!")

except Exception as e:
    print(f"❌ FAILED to connect to Xilnex source database: {e}")


# --- 2. CONFIGURE AND TEST TARGET (NEW CLOUD DB) CONNECTION ---
try:
    # THE ONLY CHANGE IS ON THE NEXT LINE
    target_connection_uri = (
        "mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
        .format(
            user=os.getenv("TARGET_USERNAME"),
            password=os.getenv("TARGET_PASSWORD"),
            server=os.getenv("TARGET_SERVER"),
            database=os.getenv("TARGET_DATABASE"),
            driver=os.getenv("TARGET_DRIVER").replace(" ", "+"),
        )
    )
    target_engine = create_engine(target_connection_uri)
    with target_engine.connect() as connection:
        print("✅ Successfully connected to new target database!")

except Exception as e:
    print(f"❌ FAILED to connect to new target database: {e}")