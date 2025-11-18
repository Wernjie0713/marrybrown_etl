import os
import pandas as pd
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

        # 3. LOAD (Corrected Logic)
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        with target_engine.connect() as connection:
            print("Clearing existing location data (preserving Unknown Location)...")
            connection.execute(text("DELETE FROM [dbo].[dim_locations] WHERE LocationKey <> -1"))
            
            print(f"Loading {len(df_final)} rows into dim_locations...")
            df_final.to_sql('dim_locations', connection, schema='dbo', if_exists='append', index=False, chunksize=1000)
            connection.commit()
        
        print("✅ ETL for dim_locations completed successfully!")
    except Exception as e:
        print(f"❌ An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    load_dotenv('.env.cloud')  # Use cloud database credentials
    main()