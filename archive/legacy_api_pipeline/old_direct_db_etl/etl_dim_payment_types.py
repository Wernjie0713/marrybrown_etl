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

def get_payment_category(method):
    """Applies business logic to categorize a payment method."""
    if not method:
        return 'Other'
    method = method.lower()
    if 'voucher' in method:
        return 'Voucher'
    if method == 'card':
        return 'Card'
    if method == 'cash':
        return 'Cash'
    if method == 'ewallet':
        return 'E-Wallet'
    return 'Other'

def main():
    """Main ETL function for the payment types dimension."""
    print("Starting ETL for dim_payment_types...")
    started_at = time.perf_counter()
    audit_client = DimensionAuditClient(lambda: get_db_engine("TARGET"), "dim_payment_types")

    try:
        # 1. EXTRACT
        print("Connecting to source...")
        source_engine = get_db_engine("XILNEX")
        
        # We query for distinct methods from the last 90 days to avoid timeouts
        # Using GROUP BY instead of DISTINCT for better performance on large tables
        sql_query = """
        SELECT
            method
        FROM
            COM_5013.APP_4_PAYMENT
        WHERE
            DATETIME__DATE >= DATEADD(day, -90, GETDATE())
            AND method IS NOT NULL AND method != ''
        GROUP BY
            method;
        """
        
        print("Extracting distinct payment methods...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} unique methods.")

        # 2. TRANSFORM
        print("Transforming data...")
        
        df.rename(columns={'method': 'PaymentMethodName'}, inplace=True)
        
        # Create the PaymentCategory column using our business rule
        df['PaymentCategory'] = df['PaymentMethodName'].apply(get_payment_category)
        
        df_final = df[['PaymentMethodName', 'PaymentCategory']]

        current_hash = dataframe_hash(df_final)
        latest = audit_client.get_latest()
        if latest and latest.source_hash == current_hash and latest.row_count == len(df_final):
            print(f"[CDC] No changes detected for dim_payment_types ({len(df_final)} rows). Skipping load.")
            return

        # 3. LOAD
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        
        with target_engine.connect() as connection:
            print("Truncating existing data from dim_payment_types...")
            connection.execute(text("TRUNCATE TABLE [dbo].[dim_payment_types]"))
            
            print(f"Loading {len(df_final)} rows into dim_payment_types...")
            df_final.to_sql(
                'dim_payment_types', 
                connection, 
                schema='dbo',
                if_exists='append', 
                index=False,
                chunksize=10000  # Optimized: added chunksize for better performance
            )
            connection.commit()

        elapsed = time.perf_counter() - started_at
        audit_client.upsert(source_hash=current_hash, row_count=len(df_final), duration_seconds=elapsed)
        print(f"[SUCCESS] ETL for dim_payment_types completed successfully in {elapsed:.2f}s!")

    except Exception as e:
        print(f"[ERROR] An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    from utils.env_loader import load_environment
    load_environment(force_local=True)  # Use .env.local for local development
    main()