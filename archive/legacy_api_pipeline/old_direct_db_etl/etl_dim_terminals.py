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
    
    connection_uri = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
    return create_engine(connection_uri, pool_pre_ping=True)

def main():
    print("Starting ETL for dim_terminals...")
    started_at = time.perf_counter()
    audit_client = DimensionAuditClient(lambda: get_db_engine("TARGET"), "dim_terminals")
    try:
        # 1. EXTRACT from APP_4_CASHIER_DRAWER
        source_engine = get_db_engine("XILNEX")
        # Using GROUP BY instead of DISTINCT for better performance on large tables
        sql_query = """
            SELECT
                cd.SITEID,
                cd.BRANCHID as LocationGUID
            FROM COM_5013.APP_4_CASHIER_DRAWER cd
            WHERE cd.SITEID IS NOT NULL
            GROUP BY
                cd.SITEID,
                cd.BRANCHID
        """
        print("Extracting terminal data from APP_4_CASHIER_DRAWER...")
        df = pd.read_sql(sql_query, source_engine)
        print(f"Extracted {len(df)} terminal records.")

        # 2. TRANSFORM
        print("Transforming terminal data...")
        
        # Get location mappings from target warehouse
        # Verify we're getting actual valid LocationKeys from the database
        target_engine = get_db_engine("TARGET")
        with target_engine.connect() as conn:
        location_map_query = "SELECT LocationKey, LocationGUID FROM dbo.dim_locations"
            location_df = pd.read_sql(location_map_query, conn)
            # Convert LocationKeys to Python int (not numpy) for consistency
            location_df['LocationKey'] = location_df['LocationKey'].apply(lambda x: int(x) if pd.notna(x) else -1)
            print(f"Loaded {len(location_df)} location mappings from dim_locations")
            
            # Check for duplicate LocationGUIDs (this could cause merge issues)
            duplicate_guids = location_df[location_df.duplicated(subset=['LocationGUID'], keep=False)]
            if len(duplicate_guids) > 0:
                print(f"WARNING: Found {len(duplicate_guids)} duplicate LocationGUIDs in dim_locations!")
                print(f"Sample duplicates:\n{duplicate_guids.head(10)}")
                # Keep only the first occurrence of each LocationGUID
                location_df = location_df.drop_duplicates(subset=['LocationGUID'], keep='first')
                print(f"After removing duplicates: {len(location_df)} location mappings remain")
        
        # Map LocationGUID to LocationKey
        # Ensure LocationGUID columns are strings and trimmed for proper matching
        df['LocationGUID'] = df['LocationGUID'].astype(str).str.strip()
        location_df['LocationGUID'] = location_df['LocationGUID'].astype(str).str.strip()
        
        df = df.merge(location_df, how='left', left_on='LocationGUID', right_on='LocationGUID')
        
        # Debug: Check merge results
        unmatched = df[df['LocationKey'].isna()]
        if len(unmatched) > 0:
            print(f"After merge: {len(unmatched)} terminals have no matching LocationGUID in dim_locations")
            print(f"Sample unmatched LocationGUIDs: {unmatched['LocationGUID'].unique()[:10].tolist()}")
        
        # Convert to Python int where possible, keep None for missing to maintain FK integrity
        df['LocationKey'] = df['LocationKey'].apply(lambda x: int(x) if pd.notna(x) else None)
        
        # Drop rows that still have no valid LocationKey after the merge
        missing_location_mask = df['LocationKey'].isna()
        missing_count = missing_location_mask.sum()
        if missing_count > 0:
            print(f"Warning: {missing_count} terminals do not have a mapped LocationKey. These rows will be dropped to preserve FK integrity.")
            print(f"Sample rows missing LocationKey:\n{df.loc[missing_location_mask, ['SITEID', 'LocationGUID']].head(10)}")
            df = df[~missing_location_mask].copy()
        
        # Validate LocationKeys exist in dim_locations (drop any orphaned references)
        valid_location_keys = {int(x) for x in location_df['LocationKey'].dropna().unique()}
        print(f"Valid LocationKeys from location_df: {sorted(list(valid_location_keys))[:20]}... (total: {len(valid_location_keys)})")
        
        invalid_mask = ~df['LocationKey'].isin(list(valid_location_keys))
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            invalid_keys = sorted(df.loc[invalid_mask, 'LocationKey'].unique().tolist())
            print(f"Warning: {invalid_count} terminals have invalid LocationKeys {invalid_keys[:20]}{'...' if len(invalid_keys) > 20 else ''}. These rows will be dropped.")
            print(f"Sample rows with invalid LocationKeys:\n{df.loc[invalid_mask, ['SITEID', 'LocationGUID', 'LocationKey']].head(10)}")
            df = df[~invalid_mask].copy()
        
        # Prepare columns
        df['TerminalID'] = df['SITEID'].astype(str)
        df['TerminalName'] = 'Terminal ' + df['SITEID'].astype(str)
        df['IsActive'] = 1  # All active by default since no IS_DELETED column
        
        # Select final columns (create a copy to avoid view issues)
        df_final = df[['TerminalID', 'LocationKey', 'TerminalName', 'IsActive']].copy()
        
        print(f"Transformed {len(df_final)} terminal records.")

        current_hash = dataframe_hash(df_final)
        latest = audit_client.get_latest()
        if latest and latest.source_hash == current_hash and latest.row_count == len(df_final):
            print(f"[CDC] No changes detected for dim_terminals ({len(df_final)} rows). Skipping load.")
            return

        # 3. LOAD
        print("Connecting to target warehouse...")
        target_engine = get_db_engine("TARGET")
        
        # Use a single connection for both validation and insert to ensure consistency
        with target_engine.connect() as connection:
            # Final validation: Query actual valid LocationKeys from database using the same connection
            valid_keys_result = connection.execute(text("SELECT DISTINCT LocationKey FROM dbo.dim_locations"))
            valid_location_keys_db = {int(row[0]) for row in valid_keys_result if row[0] is not None}
            print(f"Found {len(valid_location_keys_db)} valid LocationKeys in dim_locations")
            print(f"Sample valid LocationKeys: {sorted(list(valid_location_keys_db))[:20]}")
            
            # Check for duplicate TerminalIDs (this could cause issues)
            duplicates = df_final[df_final.duplicated(subset=['TerminalID'], keep=False)]
            if len(duplicates) > 0:
                print(f"WARNING: Found {len(duplicates)} duplicate TerminalIDs! This may cause issues.")
                print(f"Sample duplicates:\n{duplicates[['TerminalID', 'LocationKey']].head(10)}")
                # Keep only the first occurrence of each TerminalID
                df_final = df_final.drop_duplicates(subset=['TerminalID'], keep='first').copy()
                print(f"After removing duplicates: {len(df_final)} rows remain")
            
            # Ensure LocationKey column stores Python ints (while keeping track of potential nulls)
            df_final['LocationKey'] = df_final['LocationKey'].apply(lambda x: int(x) if pd.notna(x) else None)
            
            # Get unique LocationKeys in our dataframe BEFORE validation
            df_location_keys_before = {int(x) for x in df_final['LocationKey'].dropna().unique()}
            print(f"LocationKeys in dataframe BEFORE validation: {sorted(list(df_location_keys_before))[:30]}")
            
            invalid_keys = df_location_keys_before - valid_location_keys_db
            
            if invalid_keys:
                print(f"ERROR: Found {len(invalid_keys)} invalid LocationKeys in dataframe: {sorted(list(invalid_keys))}")
                print(f"These LocationKeys exist in dataframe but NOT in dim_locations!")
                # Drop rows with invalid LocationKeys to avoid FK violations
                invalid_mask = df_final['LocationKey'].isin([int(k) for k in invalid_keys])
                invalid_count = invalid_mask.sum()
                print(f"Dropping {invalid_count} rows that reference missing LocationKeys.")
                df_final = df_final[~invalid_mask].copy()
                df_final['LocationKey'] = df_final['LocationKey'].apply(lambda x: int(x) if pd.notna(x) else None)
                print(f"After dropping invalid LocationKeys, dataframe has {len(df_final)} rows.")
            
            # Final verification: ensure no invalid or missing LocationKeys remain
            invalid_post_filter = df_final[~df_final['LocationKey'].isin(list(valid_location_keys_db))]
            if len(invalid_post_filter) > 0:
                raise ValueError(f"Cannot proceed: {len(invalid_post_filter)} rows still reference invalid LocationKeys.")
            if df_final['LocationKey'].isna().any():
                raise ValueError("Cannot proceed: some terminals still lack a LocationKey after validation.")
            
            # Convert LocationKey/IsActive to SQL-friendly dtypes
            df_final['LocationKey'] = df_final['LocationKey'].astype(int)
            df_final['IsActive'] = df_final['IsActive'].astype(int)
            
            # Show summary of LocationKeys being inserted
            location_key_counts = df_final['LocationKey'].value_counts().head(10)
            print(f"Top 10 LocationKeys being inserted: {dict(location_key_counts)}")
            
            # Final pre-insert verification: Check ALL rows, not just a sample
            df_location_keys_after = {int(x) for x in df_final['LocationKey'].unique()}
            invalid_keys_after = df_location_keys_after - valid_location_keys_db
            
            if invalid_keys_after:
                print(f"FATAL: {len(invalid_keys_after)} invalid LocationKeys still present right before insert!")
                print(f"Invalid LocationKeys: {sorted(list(invalid_keys_after))}")
                invalid_rows = df_final[df_final['LocationKey'].isin(list(invalid_keys_after))]
                print(f"Rows with invalid LocationKeys:\n{invalid_rows[['TerminalID', 'LocationKey']].head(20)}")
                raise ValueError("Invalid LocationKeys detected during final validation. Aborting load.")
            
            print("Clearing existing terminal data (preserving Unknown Terminal)...")
            connection.execute(text("DELETE FROM [dbo].[dim_terminals] WHERE TerminalKey <> -1"))
            connection.commit()
            
            print(f"Loading {len(df_final)} terminals into dim_terminals...")
            # Use the same connection for insert - ensure we're using the validated dataframe
            # Final verification: print actual values being inserted (first 20 rows)
            print(f"Sample of first 20 rows being inserted:")
            print(df_final[['TerminalID', 'LocationKey']].head(20))
            
            # Use the connection for to_sql - pandas should use this connection
            # But we need to ensure the dataframe is properly formatted
            df_final.to_sql(
                'dim_terminals', 
                connection,  # Use connection to ensure same transaction
                schema='dbo', 
                if_exists='append', 
                index=False, 
                # Smaller chunk size avoids pyodbc 07002 errors caused by giant multi-value inserts
                chunksize=500
            )
            connection.commit()
        
        elapsed = time.perf_counter() - started_at
        audit_client.upsert(source_hash=current_hash, row_count=len(df_final), duration_seconds=elapsed)
        print(f"[OK] ETL for dim_terminals completed successfully in {elapsed:.2f}s!")
        
    except Exception as e:
        print(f"[ERROR] An error occurred during the ETL process: {e}")
        raise

if __name__ == "__main__":
    from utils.env_loader import load_environment
    load_environment(force_local=True)  # Use .env.local for local development
    main()

