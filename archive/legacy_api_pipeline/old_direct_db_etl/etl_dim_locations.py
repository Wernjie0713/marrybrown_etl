import os
import sys
import time
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

# Handle imports when running from different directories
try:
    from direct_db_etl.dimension_utils import DimensionAuditClient, dataframe_hash
except ImportError:
    # If running from within direct_db_etl directory, add parent to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from direct_db_etl.dimension_utils import DimensionAuditClient, dataframe_hash

def get_db_engine(prefix):
    driver = os.getenv(f"{prefix}_DRIVER").replace(" ", "+")
    server = os.getenv(f"{prefix}_SERVER")
    database = os.getenv(f"{prefix}_DATABASE")
    user = os.getenv(f"{prefix}_USERNAME")
    password = quote_plus(os.getenv(f"{prefix}_PASSWORD"))  # URL-encode password
    connection_uri = (f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes")
    return create_engine(connection_uri, pool_pre_ping=True)

def main():
    print("Starting ETL for dim_locations...")
    started_at = time.perf_counter()
    audit_client = DimensionAuditClient(lambda: get_db_engine("TARGET"), "dim_locations")
    try:
        # 1. EXTRACT
        source_engine = get_db_engine("XILNEX")
        sql_query = "SELECT ID, LOCATIONNAME, LOCATIONADDRESS, LOCATION_DELETED FROM COM_5013.LOCATION_DETAIL"
        print("Extracting data...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} rows.")

        # 2. TRANSFORM
        print("Transforming data...")
        df.rename(columns={'ID': 'LocationGUID', 'LOCATIONNAME': 'LocationName'}, inplace=True)
        df['IsActive'] = df['LOCATION_DELETED'].apply(lambda x: 1 if x == 0 else 0)
        df['City'] = None
        df['State'] = None
        df_final = df[['LocationGUID', 'LocationName', 'City', 'State', 'IsActive']]

        current_hash = dataframe_hash(df_final)
        latest = audit_client.get_latest()
        if latest and latest.source_hash == current_hash and latest.row_count == len(df_final):
            print(f"[CDC] No changes detected for dim_locations ({len(df_final)} rows). Skipping load.")
            return

        # 3. LOAD (Corrected Logic)
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        with target_engine.connect() as connection:
            print("Clearing existing location data (preserving Unknown Location)...")
            connection.execute(text("DELETE FROM [dbo].[dim_locations] WHERE LocationKey <> -1"))
            
            print(f"Loading {len(df_final)} rows into dim_locations...")
            df_final.to_sql('dim_locations', connection, schema='dbo', if_exists='append', index=False, chunksize=10000)  # Optimized: increased from 1000 to 10000
            connection.commit()
        
        elapsed = time.perf_counter() - started_at
        audit_client.upsert(source_hash=current_hash, row_count=len(df_final), duration_seconds=elapsed)
        print(f"[SUCCESS] ETL for dim_locations completed successfully in {elapsed:.2f}s!")
    except Exception as e:
        print(f"[ERROR] An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    from utils.env_loader import load_environment
    load_environment(force_local=True)  # Use .env.local for local development
    main()