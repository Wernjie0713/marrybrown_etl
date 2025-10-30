<!-- 4eaf5b0f-7196-47b8-a31c-d55de0d987e4 42aeb3f2-6f28-49fa-b978-75cdada3dd9a -->
# Parquet Export from Azure SQL Database

## Objective

Create Python scripts to export Xilnex sales data (sales, sales_items, payments) for one month from Azure SQL Database directly to Parquet format in local directory `C:\exports\` for testing purposes.

## Key Difference for Azure SQL

Since you're using Azure SQL Database (not on-premises SQL Server):

- No xp_cmdshell or BCP from within database
- Export must be done client-side using Python
- Direct query-to-Parquet conversion (no CSV intermediate)

## Implementation Steps

### 1. Install Required Python Packages

```bash
pip install pyodbc pandas pyarrow sqlalchemy
```

### 2. Create Azure SQL Connection Configuration

Create `config.py`:

```python
# Azure SQL Database connection settings
AZURE_SQL_CONFIG = {
    'server': 'your-server.database.windows.net',  # Replace with your server
    'database': 'COM_5013',  # Your Xilnex database name
    'username': 'your_username',  # Replace with your username
    'password': 'your_password',  # Replace with your password
    'driver': '{ODBC Driver 17 for SQL Server}'  # or 18
}

# Export settings
EXPORT_DIR = 'C:/exports'
MONTH_TO_EXPORT = '2025-10'  # October 2025
```

### 3. Create Main Export Script

Create `export_to_parquet.py`:

```python
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
        ORDER_SOURCE,
        double_total_bill_discount_amount,
        STRING_EXTEND_3 as terminal_id,
        SALES_STATUS
    FROM APP_4_SALES
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
        double_sub_total,
        double_cost,
        voucher_no
    FROM APP_4_SALESITEM
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
        DATETIME__PAYMENT_DATE
    FROM APP_4_PAYMENT
    WHERE DATETIME__PAYMENT_DATE >= '{start_date}'
        AND DATETIME__PAYMENT_DATE < '{end_date}'
    """
    
    payments_df = export_table_to_parquet(
        payments_query,
        f"payments_{year}{month}.parquet"
    )
    
    print("\n" + "="*60)
    print("Export Summary:")
    print(f"  Sales: {len(sales_df):,} rows")
    print(f"  Sales Items: {len(items_df):,} rows")
    print(f"  Payments: {len(payments_df):,} rows")
    print("\nExport complete!")

if __name__ == "__main__":
    main()
```

### 4. Create Validation Script

Create `validate_parquet.py`:

```python
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import config

def validate_parquet_files():
    """Validate exported Parquet files"""
    export_dir = Path(config.EXPORT_DIR)
    year, month = config.MONTH_TO_EXPORT.split('-')
    
    files = [
        f"sales_{year}{month}.parquet",
        f"sales_items_{year}{month}.parquet",
        f"payments_{year}{month}.parquet"
    ]
    
    print("Parquet File Validation")
    print("="*80)
    
    for filename in files:
        filepath = export_dir / filename
        
        if not filepath.exists():
            print(f"\n❌ {filename}: FILE NOT FOUND")
            continue
        
        # Read Parquet file
        df = pd.read_parquet(filepath)
        
        # Get file info
        file_size_mb = filepath.stat().st_size / 1024 / 1024
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        print(f"\n✓ {filename}")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        print(f"  File Size: {file_size_mb:.2f} MB")
        print(f"  Memory Usage: {memory_mb:.2f} MB")
        print(f"  Compression Ratio: {memory_mb / file_size_mb:.2f}x")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        
        # Show data types
        print(f"\n  Data Types:")
        for col, dtype in df.dtypes.items():
            print(f"    {col}: {dtype}")
        
        # Show date range if applicable
        if 'datetime__sales_date' in df.columns:
            print(f"\n  Date Range:")
            print(f"    Min: {df['datetime__sales_date'].min()}")
            print(f"    Max: {df['datetime__sales_date'].max()}")
        elif 'DATETIME__PAYMENT_DATE' in df.columns:
            print(f"\n  Date Range:")
            print(f"    Min: {df['DATETIME__PAYMENT_DATE'].min()}")
            print(f"    Max: {df['DATETIME__PAYMENT_DATE'].max()}")
        
        # Show sample data
        print(f"\n  Sample (first 3 rows):")
        print(df.head(3).to_string())

if __name__ == "__main__":
    validate_parquet_files()
```

### 5. Create Test Query Script (Optional)

Create `test_azure_connection.py` to verify connection before bulk export:

```python
import pyodbc
import config

def test_connection():
    """Test Azure SQL Database connection"""
    try:
        print("Testing Azure SQL Database connection...")
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
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"\n✓ Connection successful!")
        print(f"  Database: {config.AZURE_SQL_CONFIG['database']}")
        print(f"  Server: {config.AZURE_SQL_CONFIG['server']}")
        print(f"  Version: {version[:50]}...")
        
        # Get record counts
        year, month = config.MONTH_TO_EXPORT.split('-')
        start_date = f"{year}-{month}-01"
        
        if month == '12':
            end_date = f"{int(year)+1}-01-01"
        else:
            end_date = f"{year}-{int(month)+1:02d}-01"
        
        print(f"\n  Checking record counts for {config.MONTH_TO_EXPORT}:")
        
        cursor.execute(f"""
            SELECT COUNT(*) FROM APP_4_SALES 
            WHERE DATETIME__SALES_DATE >= '{start_date}' 
            AND DATETIME__SALES_DATE < '{end_date}'
        """)
        sales_count = cursor.fetchone()[0]
        print(f"    Sales: {sales_count:,} records")
        
        cursor.execute(f"""
            SELECT COUNT(*) FROM APP_4_SALESITEM 
            WHERE datetime__sales_date >= '{start_date}' 
            AND datetime__sales_date < '{end_date}'
        """)
        items_count = cursor.fetchone()[0]
        print(f"    Sales Items: {items_count:,} records")
        
        cursor.execute(f"""
            SELECT COUNT(*) FROM APP_4_PAYMENT 
            WHERE DATETIME__PAYMENT_DATE >= '{start_date}' 
            AND DATETIME__PAYMENT_DATE < '{end_date}'
        """)
        payments_count = cursor.fetchone()[0]
        print(f"    Payments: {payments_count:,} records")
        
        conn.close()
        print("\n✓ Connection test complete!")
        
    except Exception as e:
        print(f"\n❌ Connection failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_connection()
```

### 6. Usage Instructions

**Step 1: Update Configuration**

Edit `config.py` with your Azure SQL Database credentials:

```python
AZURE_SQL_CONFIG = {
    'server': 'your-xilnex-server.database.windows.net',
    'database': 'COM_5013',
    'username': 'your_username',
    'password': 'your_password',
    'driver': '{ODBC Driver 17 for SQL Server}'
}
```

**Step 2: Test Connection**

```bash
python test_azure_connection.py
```

**Step 3: Run Export**

```bash
python export_to_parquet.py
```

**Step 4: Validate Results**

```bash
python validate_parquet.py
```

## Files to Create

1. `config.py` - Azure SQL connection configuration
2. `export_to_parquet.py` - Main export script (queries Azure SQL, writes Parquet)
3. `validate_parquet.py` - Validation and inspection script
4. `test_azure_connection.py` - Connection test script

## Expected Performance

For Azure SQL Database:

- Export speed depends on network bandwidth and Azure SQL tier
- Typical speeds: 10,000-50,000 rows/second
- Parquet compression: 3-5x smaller than CSV
- For 1 month of data (estimated 100K-500K rows per table):
  - Export time: 1-5 minutes
  - File sizes: 5-50 MB per table (compressed)

## Benefits of This Approach

1. **No intermediate CSV files** - Direct query-to-Parquet
2. **Chunked processing** - Handles large datasets without memory issues
3. **Progress tracking** - Shows row counts during export
4. **Compression** - Snappy compression reduces file sizes
5. **Type preservation** - Maintains data types from database
6. **Validation built-in** - Easy to verify exported data

## Next Steps After Testing

1. Measure actual export performance for your data volume
2. Test loading Parquet files into your data warehouse
3. Compare Parquet vs current Python ETL approach (performance, reliability)
4. If successful, plan for:

   - Full historical bulk export (all dates)
   - CDC setup for incremental updates
   - Automated daily/hourly export scripts

### To-dos

- [ ] Run SQL Server version check query to determine export approach
- [ ] Enable xp_cmdshell and create export directory with permissions
- [ ] Run BCP export queries for sales, sales_items, and payments (October 2025)
- [ ] Run Python script to convert CSV files to Parquet format
- [ ] Run validation queries to verify record counts match
- [ ] Test loading Parquet files with Pandas to verify format