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

    return create_engine(connection_uri, pool_pre_ping=True)

def main():
    """Main ETL function for the products dimension."""
    print("Starting ETL for dim_products...")
    started_at = time.perf_counter()
    audit_client = DimensionAuditClient(lambda: get_db_engine("TARGET"), "dim_products")

    try:
        # 1. EXTRACT
        print("Connecting to source...")
        source_engine = get_db_engine("XILNEX") # <-- THIS LINE WAS MISSING
        
        sql_query = """
        SELECT ID, ITEM_CODE, ITEM_NAME, CATEGORY, ITEM_TYPE, ITEM_BRAND,
               DOUBLE_SALE_PRICE, BOOL_ISPACKAGE, BOOL_DISABLED
        FROM COM_5013.APP_4_ITEM
        """
        
        print("Extracting data from app_4_item...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} rows.")

        # 2. TRANSFORM
        print("Transforming data...")
        
        df.rename(columns={
            'ID': 'SourceProductID',
            'ITEM_CODE': 'ProductCode',
            'ITEM_NAME': 'ProductName',
            'CATEGORY': 'Category',
            'ITEM_TYPE': 'ProductType',
            'ITEM_BRAND': 'Brand',
            'DOUBLE_SALE_PRICE': 'CurrentSalePrice',
            'BOOL_ISPACKAGE': 'IsPackage'
        }, inplace=True)
        
        df['IsActive'] = df['BOOL_DISABLED'].apply(lambda x: 1 if x == 0 else 0)
        df['IsPackage'] = df['IsPackage'].fillna(0).astype(bool)
        
        df_final = df[[
            'SourceProductID', 'ProductCode', 'ProductName', 'Category', 'ProductType',
            'Brand', 'CurrentSalePrice', 'IsPackage', 'IsActive'
        ]]

        initial_rows = len(df_final)
        # Deduplicate by SourceProductID (ID) instead of ProductCode (ITEM_CODE)
        # Because sales transactions reference products by ID, not ITEM_CODE
        # This ensures all products are loaded, even if they share ITEM_CODE with others
        df_final = df_final.drop_duplicates(subset=['SourceProductID'], keep='first')
        if initial_rows > len(df_final):
            print(f"Removed {initial_rows - len(df_final)} duplicate product IDs (should be 0).")

        current_hash = dataframe_hash(df_final)
        latest = audit_client.get_latest()
        if latest and latest.source_hash == current_hash and latest.row_count == len(df_final):
            print(f"[CDC] No changes detected for dim_products ({len(df_final)} rows). Skipping load.")
            return

        # 3. LOAD
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        
        with target_engine.connect() as connection:
            print("Truncating existing data from dim_products...")
            connection.execute(text("TRUNCATE TABLE [dbo].[dim_products]"))
            
            print(f"Loading {len(df_final)} unique rows into dim_products...")
            df_final.to_sql('dim_products', connection, schema='dbo', if_exists='append', index=False, chunksize=10000)  # Optimized: increased from 1000 to 10000
            connection.commit()
        
        elapsed = time.perf_counter() - started_at
        audit_client.upsert(source_hash=current_hash, row_count=len(df_final), duration_seconds=elapsed)
        print(f"[SUCCESS] ETL for dim_products completed successfully in {elapsed:.2f}s!")

    except Exception as e:
        print(f"[ERROR] An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    from utils.env_loader import load_environment
    load_environment(force_local=True)  # Use .env.local for local development
    main()