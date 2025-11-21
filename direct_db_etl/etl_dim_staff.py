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
    """Creates a SQLAlchemy engine from .env credentials."""
    driver = os.getenv(f"{prefix}_DRIVER").replace(" ", "+")
    server = os.getenv(f"{prefix}_SERVER")
    database = os.getenv(f"{prefix}_DATABASE")
    user = os.getenv(f"{prefix}_USERNAME")
    password = quote_plus(os.getenv(f"{prefix}_PASSWORD"))
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    )
    if "TARGET" in prefix:
         connection_uri += "&TrustServerCertificate=yes"

    return create_engine(connection_uri)

def get_staff_type(cashier_name):
    """Applies business logic to categorize a staff member."""
    if not cashier_name:
        return 'Human'
    name = cashier_name.lower()
    if 'panda' in name or 'grab' in name:
        return 'Integration'
    if 'kiosk' in name:
        return 'System'
    return 'Human'

def main():
    """Main ETL function for the staff dimension."""
    print("Starting ETL for dim_staff...")
    started_at = time.perf_counter()
    audit_client = DimensionAuditClient(lambda: get_db_engine("TARGET"), "dim_staff")

    try:
        # 1. EXTRACT
        print("Connecting to source...")
        source_engine = get_db_engine("XILNEX")
        
        # Query for distinct staff from the last 90 days to avoid timeouts
        # Using GROUP BY instead of DISTINCT for better performance on large tables
        sql_query = """
        SELECT
            CASHIER,
            SALES_PERSON,
            SALES_PERSON_USERNAME
        FROM
            COM_5013.APP_4_SALES
        WHERE
            DATETIME__SALES_DATE >= DATEADD(day, -90, GETDATE())
        GROUP BY
            CASHIER,
            SALES_PERSON,
            SALES_PERSON_USERNAME;
        """
        
        print("Extracting distinct staff members...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} unique staff records.")

        # 2. TRANSFORM
        print("Transforming data...")
        
        # Create StaffUsername using COALESCE logic
        df['StaffUsername'] = df['SALES_PERSON_USERNAME'].fillna(df['CASHIER'])

        # Create StaffFullName using COALESCE logic
        df['StaffFullName'] = df['CASHIER'].fillna(df['SALES_PERSON'])

        # Create StaffType using our business rule
        df['StaffType'] = df['CASHIER'].apply(get_staff_type)
        
        # Drop rows where the final username is still null or empty
        df.dropna(subset=['StaffUsername'], inplace=True)
        df = df[df['StaffUsername'] != '']

        # Select and reorder columns for the final dimension table
        df_final = df[['StaffUsername', 'StaffFullName', 'StaffType']].drop_duplicates(subset=['StaffUsername'])

        current_hash = dataframe_hash(df_final)
        latest = audit_client.get_latest()
        if latest and latest.source_hash == current_hash and latest.row_count == len(df_final):
            print(f"[CDC] No changes detected for dim_staff ({len(df_final)} rows). Skipping load.")
            return

        # 3. LOAD
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        
        with target_engine.connect() as connection:
            print("Truncating existing data from dim_staff...")
            connection.execute(text("TRUNCATE TABLE [dbo].[dim_staff]"))
            
            print(f"Loading {len(df_final)} rows into dim_staff...")
            df_final.to_sql(
                'dim_staff', 
                connection, 
                schema='dbo',
                if_exists='append', 
                index=False,
                chunksize=10000  # Optimized: added chunksize for better performance
            )
            
            print("Inserting 'Unspecified Staff' record...")
            connection.execute(text("SET IDENTITY_INSERT [dbo].[dim_staff] ON;"))
            connection.execute(text("""
                IF NOT EXISTS (SELECT 1 FROM [dbo].[dim_staff] WHERE StaffKey = -1)
                BEGIN
                    INSERT INTO [dbo].[dim_staff] (StaffKey, StaffUsername, StaffFullName, StaffType)
                    VALUES (-1, 'N/A', 'Unspecified Staff', 'Unknown');
                END
            """))
            connection.execute(text("SET IDENTITY_INSERT [dbo].[dim_staff] OFF;"))
            connection.commit()

        elapsed = time.perf_counter() - started_at
        audit_client.upsert(source_hash=current_hash, row_count=len(df_final), duration_seconds=elapsed)
        print(f"[SUCCESS] ETL for dim_staff completed successfully in {elapsed:.2f}s!")

    except Exception as e:
        print(f"[ERROR] An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    from utils.env_loader import load_environment
    load_environment(force_local=True)  # Use .env.local for local development
    main()