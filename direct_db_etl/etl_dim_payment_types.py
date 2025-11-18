import os
import pandas as pd
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

    try:
        # 1. EXTRACT
        print("Connecting to source...")
        source_engine = get_db_engine("XILNEX")
        
        # We query for DISTINCT methods from the last 90 days to avoid timeouts
        sql_query = """
        SELECT DISTINCT
            method
        FROM
            COM_5013.APP_4_PAYMENT
        WHERE
            DATETIME__DATE >= DATEADD(day, -90, GETDATE())
            AND method IS NOT NULL AND method != '';
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
                index=False
            )
            connection.commit()

        print("✅ ETL for dim_payment_types completed successfully!")

    except Exception as e:
        print(f"❌ An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    load_dotenv('.env.cloud')  # Use cloud database credentials
    main()