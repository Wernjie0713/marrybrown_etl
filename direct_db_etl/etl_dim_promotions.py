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

def get_promo_type(type_id):
    """Maps the numeric voucher type to a descriptive string."""
    if type_id == 1:
        return 'Product Deal'
    if type_id == 2:
        return 'Discount Voucher'
    if type_id == 4:
        return 'Gift Voucher'
    return 'Unknown'

def main():
    """Main ETL function for the promotions dimension."""
    print("Starting ETL for dim_promotions...")

    try:
        # 1. EXTRACT
        print("Connecting to source...")
        source_engine = get_db_engine("XILNEX")
        
        sql_query = """
        SELECT
            ID,
            RULE_TITLE,
            RULE_DESCRIPTION,
            REWARD_CODE,
            VOUCHER_MASTER_TYPE,
            DATETIME__ACTIVATE_DATE,
            DATETIME__EXPIRY_DATE,
            IS_ACTIVATE,
            WBDELETED
        FROM
            COM_5013.APP_4_VOUCHER_MASTER
        """
        
        print("Extracting data from APP_4_VOUCHER_MASTER...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} rows.")

        # 2. TRANSFORM
        print("Transforming data...")
        
        df.rename(columns={
            'ID': 'SourcePromotionID',
            'RULE_TITLE': 'PromotionName',
            'RULE_DESCRIPTION': 'PromotionDescription',
            'REWARD_CODE': 'PromotionCode',
            'DATETIME__ACTIVATE_DATE': 'StartDate',
            'DATETIME__EXPIRY_DATE': 'EndDate'
        }, inplace=True)
        
        # Create PromotionType using our mapping logic
        df['PromotionType'] = df['VOUCHER_MASTER_TYPE'].apply(get_promo_type)

        # Create the IsActive flag
        df['IsActive'] = df.apply(lambda row: 1 if row['IS_ACTIVATE'] == 1 and row['WBDELETED'] == 0 else 0, axis=1)
        
        # Select and reorder columns for the final dimension table
        df_final = df[[
            'SourcePromotionID', 'PromotionName', 'PromotionDescription', 'PromotionCode',
            'PromotionType', 'StartDate', 'EndDate', 'IsActive'
        ]]

        # 3. LOAD
        print("Connecting to target...")
        target_engine = get_db_engine("TARGET")
        
        with target_engine.connect() as connection:
            print("Truncating existing data from dim_promotions...")
            connection.execute(text("TRUNCATE TABLE [dbo].[dim_promotions]"))
            
            print(f"Loading {len(df_final)} rows into dim_promotions...")
            df_final.to_sql(
                'dim_promotions', 
                connection, 
                schema='dbo',
                if_exists='append', 
                index=False
            )
            
            print("Inserting 'No Promotion' record...")
            connection.execute(text("SET IDENTITY_INSERT [dbo].[dim_promotions] ON;"))
            connection.execute(text("""
                IF NOT EXISTS (SELECT 1 FROM [dbo].[dim_promotions] WHERE PromotionKey = -1)
                BEGIN
                    INSERT INTO [dbo].[dim_promotions] (PromotionKey, SourcePromotionID, PromotionName, IsActive)
                    VALUES (-1, -1, 'No Promotion', 1);
                END
            """))
            connection.execute(text("SET IDENTITY_INSERT [dbo].[dim_promotions] OFF;"))
            connection.commit()

        print("✅ ETL for dim_promotions completed successfully!")

    except Exception as e:
        print(f"❌ An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    load_dotenv('.env.cloud')  # Use cloud database credentials
    main()