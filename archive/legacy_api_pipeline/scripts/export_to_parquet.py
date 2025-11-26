import pyodbc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import config

def create_connection():
    """Create connection to Azure SQL Database"""
    conn_str = (
        f"DRIVER={config.AZURE_SQL_CONFIG['driver']};"
        f"SERVER={config.AZURE_SQL_CONFIG['server']};"
        f"DATABASE={config.AZURE_SQL_CONFIG['database']};"
        f"UID={config.AZURE_SQL_CONFIG['username']};"
        f"PWD={config.AZURE_SQL_CONFIG['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def export_table_to_parquet(query, output_file, chunk_size=50000):
    """
    Export Azure SQL query results to Parquet file with chunking
    for large datasets
    """
    print(f"\nExporting to {output_file}...")
    start_time = datetime.now()
    
    # Create export directory if not exists
    Path(config.EXPORT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Read in chunks to handle large datasets
    conn = create_connection()
    chunks = []
    total_rows = 0
    
    for chunk in pd.read_sql_query(query, conn, chunksize=chunk_size):
        chunks.append(chunk)
        total_rows += len(chunk)
        print(f"  Processed {total_rows:,} rows...", end='\r')
    
    conn.close()
    
    # Combine all chunks
    df = pd.concat(chunks, ignore_index=True)
    
    # Write to Parquet
    output_path = Path(config.EXPORT_DIR) / output_file
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    elapsed = (datetime.now() - start_time).total_seconds()
    file_size_mb = output_path.stat().st_size / 1024 / 1024
    
    print(f"\n  ✓ Exported {total_rows:,} rows in {elapsed:.2f} seconds")
    print(f"  ✓ File size: {file_size_mb:.2f} MB")
    print(f"  ✓ Compression ratio: {(df.memory_usage(deep=True).sum() / 1024 / 1024) / file_size_mb:.2f}x")
    
    return df

def export_recipe_costs():
    """
    Export pre-calculated recipe costs for combo items from APP_4_RECIPESUMMARY.
    This is a one-time export (not date-specific) needed for cost calculation fix.
    
    FIXED: APP_4_RECIPESUMMARY is a transaction history table, not a master recipe.
    We use ROW_NUMBER() to get only the LATEST version of each ingredient.
    """
    print(f"\nExporting recipe costs for combo items...")
    start_time = datetime.now()
    
    # Create export directory if not exists
    Path(config.EXPORT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Query recipe costs from Xilnex - GET LATEST RECIPE VERSION ONLY
    recipe_query = """
        WITH LatestRecipeIngredients AS (
            SELECT 
                rs.ITEM_ID,
                rs.SALES_TYPE,
                rs.RM_ITEM_ID,
                rs.DOUBLE_QUANTITY,
                rs.DOUBLE_COST,
                ROW_NUMBER() OVER (
                    PARTITION BY rs.ITEM_ID, rs.SALES_TYPE, rs.RM_ITEM_ID 
                    ORDER BY rs.DATETIME__TRANSACTION_DATETIME DESC
                ) as rn
            FROM COM_5013.APP_4_RECIPESUMMARY rs
            WHERE rs.DOUBLE_COST IS NOT NULL
              AND rs.DOUBLE_QUANTITY IS NOT NULL
        )
        SELECT 
            i.ID as item_id,
            lr.SALES_TYPE as sales_type,
            ISNULL(SUM(ISNULL(lr.DOUBLE_QUANTITY, 0) * ISNULL(lr.DOUBLE_COST, 0)), 0) as calculated_recipe_cost,
            COUNT(lr.RM_ITEM_ID) as ingredient_count
        FROM COM_5013.APP_4_ITEM i
        JOIN LatestRecipeIngredients lr ON lr.ITEM_ID = i.ID AND lr.rn = 1
        WHERE i.BOOL_ISPACKAGE = 1
        GROUP BY i.ID, lr.SALES_TYPE
    """
    
    conn = create_connection()
    df = pd.read_sql_query(recipe_query, conn)
    conn.close()
    
    # Write to Parquet
    output_path = Path(config.EXPORT_DIR) / "recipe_costs.parquet"
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    elapsed = (datetime.now() - start_time).total_seconds()
    file_size_mb = output_path.stat().st_size / 1024 / 1024
    
    print(f"  ✓ Exported {len(df):,} recipe cost records in {elapsed:.2f} seconds")
    print(f"  ✓ File size: {file_size_mb:.2f} MB")
    
    return df

def main():
    """Main export function"""
    year, month = config.MONTH_TO_EXPORT.split('-')
    start_date = f"{year}-{month}-01"
    
    # Calculate end date (last day of month)
    if month == '12':
        end_date = f"{int(year)+1}-01-01"
    else:
        end_date = f"{year}-{int(month)+1:02d}-01"
    
    print(f"Exporting data for {config.MONTH_TO_EXPORT}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Export directory: {config.EXPORT_DIR}")
    print("="*60)
    
    # Export Sales table
    sales_query = f"""
    SELECT 
        id,
        sales_no,
        datetime__sales_date,
        now_time,
        sale_location,
        CUSTOMER_ID,
        CASHIER,
        SALES_PERSON,
        SALES_PERSON_USERNAME,
        SALES_TYPE,
        SUBSALES_TYPE,
        ORDER_SOURCE,
        double_total_bill_discount_amount,
        STRING_EXTEND_3 as terminal_id,
        SALES_STATUS
    FROM COM_5013.APP_4_SALES
    WHERE DATETIME__SALES_DATE >= '{start_date}'
        AND DATETIME__SALES_DATE < '{end_date}'
    """
    
    sales_df = export_table_to_parquet(
        sales_query, 
        f"sales_{year}{month}.parquet"
    )
    
    # Export Sales Items table
    items_query = f"""
    SELECT 
        id,
        item_code,
        item_name,
        sales_no,
        datetime__sales_date,
        INT_QUANTITY,
        double_price,
        double_total_discount_amount,
        double_mgst_tax_amount,
        DOUBLE_TOTAL_TAX_AMOUNT,
        double_sub_total,
        double_cost,
        voucher_no
    FROM COM_5013.APP_4_SALESITEM
    WHERE datetime__sales_date >= '{start_date}'
        AND datetime__sales_date < '{end_date}'
    """
    
    items_df = export_table_to_parquet(
        items_query,
        f"sales_items_{year}{month}.parquet"
    )
    
    # Export Payments table
    payments_query = f"""
    SELECT 
        id,
        invoice_id,
        method,
        double_amount,
        STRING_EXTEND_2 as card_type,
        DATETIME__DATE
    FROM COM_5013.APP_4_PAYMENT
    WHERE DATETIME__DATE >= '{start_date}'
        AND DATETIME__DATE < '{end_date}'
    """
    
    payments_df = export_table_to_parquet(
        payments_query,
        f"payments_{year}{month}.parquet"
    )
    
    # Export Recipe Costs (one-time, not date-specific)
    recipe_df = export_recipe_costs()
    
    print("\n" + "="*60)
    print("Export Summary:")
    print(f"  Sales: {len(sales_df):,} rows")
    print(f"  Sales Items: {len(items_df):,} rows")
    print(f"  Payments: {len(payments_df):,} rows")
    print(f"  Recipe Costs: {len(recipe_df):,} rows")
    print(f"\n  Total Exported: {len(sales_df) + len(items_df) + len(payments_df) + len(recipe_df):,} rows")
    print("\nExport complete!")

if __name__ == "__main__":
    main()

