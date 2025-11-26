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

def create_full_name(row):
    """Combines name fields into a single FullName."""
    first = row['FIRST_NAME'] or ''
    last = row['LAST_NAME'] or ''
    company = row['COMPANY_NAME'] or ''
    full_name_from_parts = f"{first} {last}".strip()
    return full_name_from_parts if full_name_from_parts else company

def main():
    """Main ETL function for the customers dimension."""
    print("Starting ETL for dim_customers...")
    started_at = time.perf_counter()
    audit_client = DimensionAuditClient(lambda: get_db_engine("TARGET"), "dim_customers")

    try:
        # 1. EXTRACT
        source_engine = get_db_engine("XILNEX")
        sql_query = """
        SELECT ID, CUSTOMER_CODE, FIRST_NAME, LAST_NAME, COMPANY_NAME,
               EMAIL, MOBILE, CUSTOMER_GROUP, double_accumulate_value,
               DATETIME__CREATION_DATE, DATETIME__DOB, BOOL_ACTIVE
        FROM COM_5013.APP_4_CUSTOMER
        """
        print("Extracting data...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} rows.")

        # 2. TRANSFORM
        print("Transforming data...")
        df.rename(columns={'ID': 'CustomerGUID', 'CUSTOMER_CODE': 'CustomerCode', 'MOBILE': 'MobileNumber', 'EMAIL': 'Email', 'CUSTOMER_GROUP': 'CustomerGroup', 'double_accumulate_value': 'CurrentLoyaltyPoints', 'DATETIME__CREATION_DATE': 'RegistrationDate', 'DATETIME__DOB': 'DateOfBirth'}, inplace=True)
        df['FullName'] = df.apply(create_full_name, axis=1)
        df['FirstName'] = df['FullName'].apply(lambda x: x.split(' ')[0] if pd.notna(x) else None)
        df['LastName'] = df['FullName'].apply(lambda x: ' '.join(x.split(' ')[1:]) if pd.notna(x) and ' ' in x else None)
        df['DateOfBirth'] = pd.to_datetime(df['DateOfBirth'], errors='coerce').dt.date
        df['IsActive'] = df['BOOL_ACTIVE'].apply(lambda x: 1 if x == 1 else 0)
        df_final = df[['CustomerGUID', 'CustomerCode', 'FullName', 'FirstName', 'LastName', 'MobileNumber', 'Email', 'CustomerGroup', 'CurrentLoyaltyPoints', 'RegistrationDate', 'DateOfBirth', 'IsActive']]

        current_hash = dataframe_hash(df_final)
        latest = audit_client.get_latest()
        if latest and latest.source_hash == current_hash and latest.row_count == len(df_final):
            print(f"[CDC] No changes detected for dim_customers ({len(df_final)} rows). Skipping load.")
            return

        # 3. LOAD
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        
        with target_engine.connect() as connection:
            print("Truncating existing data from dim_customers...")
            connection.execute(text("TRUNCATE TABLE [dbo].[dim_customers]"))
            
            print(f"Loading {len(df_final)} rows into dim_customers...")
            df_final.to_sql(
                'dim_customers', 
                connection, 
                schema='dbo',
                if_exists='append', 
                index=False,
                chunksize=10000  # Optimized: increased from 1000 to 10000 for better performance
            )
            
            print("Inserting 'Unknown Customer' record...")
            connection.execute(text("SET IDENTITY_INSERT [dbo].[dim_customers] ON;"))
            connection.execute(text("""
                INSERT INTO [dbo].[dim_customers] (CustomerKey, CustomerGUID, FullName, IsActive)
                VALUES (-1, -1, 'Unknown Customer', 1);
            """))
            connection.execute(text("SET IDENTITY_INSERT [dbo].[dim_customers] OFF;"))
            connection.commit()

        elapsed = time.perf_counter() - started_at
        audit_client.upsert(source_hash=current_hash, row_count=len(df_final), duration_seconds=elapsed)
        print(f"[SUCCESS] ETL for dim_customers completed successfully in {elapsed:.2f}s!")

    except Exception as e:
        print(f"[ERROR] An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    from utils.env_loader import load_environment
    load_environment(force_local=True)  # Use .env.local for local development
    main()