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
    
    connection_uri = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
    return create_engine(connection_uri, pool_pre_ping=True)

def main():
    print("Starting ETL for dim_terminals...")
    try:
        # 1. EXTRACT from APP_4_CASHIER_DRAWER
        source_engine = get_db_engine("XILNEX")
        sql_query = """
            SELECT DISTINCT
                cd.SITEID,
                cd.BRANCHID as LocationGUID
            FROM COM_5013.APP_4_CASHIER_DRAWER cd
            WHERE cd.SITEID IS NOT NULL
        """
        print("Extracting terminal data from APP_4_CASHIER_DRAWER...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} terminal records.")

        # 2. TRANSFORM
        print("Transforming terminal data...")
        
        # Get location mappings from target warehouse
        target_engine = get_db_engine("TARGET")
        location_map_query = "SELECT LocationKey, LocationGUID FROM dbo.dim_locations"
        location_df = pd.read_sql(location_map_query, target_engine)
        
        # Map LocationGUID to LocationKey
        df = df.merge(location_df, how='left', left_on='LocationGUID', right_on='LocationGUID')
        
        # Handle missing LocationKey (use -1 for unknown)
        df['LocationKey'] = df['LocationKey'].fillna(-1).astype(int)
        
        # Prepare columns
        df['TerminalID'] = df['SITEID'].astype(str)
        df['TerminalName'] = 'Terminal ' + df['SITEID'].astype(str)
        df['IsActive'] = 1  # All active by default since no IS_DELETED column
        
        # Select final columns
        df_final = df[['TerminalID', 'LocationKey', 'TerminalName', 'IsActive']]
        
        print(f"Transformed {len(df_final)} terminal records.")

        # 3. LOAD
        print("Connecting to target warehouse...")
        with target_engine.connect() as connection:
            # Truncate existing data (except the -1 record)
            print("Clearing existing terminal data (preserving Unknown Terminal)...")
            connection.execute(text("DELETE FROM [dbo].[dim_terminals] WHERE TerminalKey <> -1"))
            
            # Load new data
            print(f"Loading {len(df_final)} terminals into dim_terminals...")
            df_final.to_sql(
                'dim_terminals', 
                connection, 
                schema='dbo', 
                if_exists='append', 
                index=False, 
                chunksize=1000
            )
            connection.commit()
        
        print("✅ ETL for dim_terminals completed successfully!")
        
    except Exception as e:
        print(f"❌ An error occurred during the ETL process: {e}")
        raise

if __name__ == "__main__":
    load_dotenv('.env.cloud')  # Use cloud database credentials
    main()

