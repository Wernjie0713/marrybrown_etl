import os
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import psutil
except ImportError:  # pragma: no cover - psutil optional
    psutil = None

# --- CONFIGURATION ---
# Phase 2: Parquet-based ETL (Offline Processing)
# Processing September 2025 data from exported Parquet files
START_DATE = date(2025, 9, 1)
END_DATE = date(2025, 9, 30)
CHUNK_SIZE = 20000  # Process 20,000 rows at a time in memory
WAREHOUSE_SESSION_CAP = int(os.getenv("WAREHOUSE_SESSION_CAP", "8"))


def determine_worker_count() -> int:
    env_override = os.getenv("HISTORICAL_MAX_WORKERS")
    if env_override:
        return max(1, int(env_override))

    cpu_count = os.cpu_count() or 2
    max_by_cpu = max(1, cpu_count - 1)

    max_by_memory = 4
    if psutil:
        available_gb = psutil.virtual_memory().available / (1024 ** 3)
        if available_gb < 4:
            max_by_memory = 1
        elif available_gb < 8:
            max_by_memory = 2
        elif available_gb < 16:
            max_by_memory = 3
    session_cap = max(1, WAREHOUSE_SESSION_CAP - 1)
    return max(1, min(max_by_cpu, max_by_memory, session_cap))


MAX_WORKERS = determine_worker_count()
EXPORT_DIR = Path("C:/exports")  # Directory containing exported Parquet files
MONTH_TO_PROCESS = "202509"  # September 2025

def get_db_engine(prefix):
    """Creates a SQLAlchemy engine from .env credentials.
    
    MULTITHREADING FIX: Configures proper connection pooling to handle 
    multiple worker threads each creating their own engine instances.
    """
    driver = os.getenv(f"{prefix}_DRIVER").replace(" ", "+")
    server = os.getenv(f"{prefix}_SERVER")
    database = os.getenv(f"{prefix}_DATABASE")
    user = os.getenv(f"{prefix}_USERNAME")
    password = os.getenv(f"{prefix}_PASSWORD")
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    )
    if "TARGET" in prefix:
         connection_uri += "&TrustServerCertificate=yes"
    
    # CRITICAL: Proper connection pool settings for multithreading
    # - pool_size: Max connections to keep open (per engine instance)
    # - max_overflow: Additional connections allowed beyond pool_size
    # - pool_pre_ping: Test connections before using (prevent stale connections)
    # - pool_recycle: Recycle connections after 1 hour to prevent timeouts
    return create_engine(
        connection_uri, 
        pool_size=5,           # Base pool of 5 connections per engine
        max_overflow=10,       # Allow up to 10 additional connections if needed
        pool_pre_ping=True,    # Test connection health before use
        pool_recycle=3600      # Recycle connections every hour
    )

def process_single_day(day_to_process, sales_df, items_df, payments_df):
    """Loads data for a single day from pre-loaded Parquet DataFrames.
    
    NEW APPROACH: Read from local Parquet files (offline processing)
    - No Xilnex database connection needed
    - Much faster (local disk vs. network)
    - No IP firewall issues
    """
    day_str = day_to_process.strftime('%Y-%m-%d')
    start_ts = time.perf_counter()
    
    total_sales_rows = 0
    total_salesitem_rows = 0
    total_payment_rows = 0

    try:
        # Create isolated warehouse engine for THIS thread only
        target_engine = get_db_engine("TARGET")
        
        # --- FILTER & LOAD SALES for this day ---
        # Convert to datetime first, then compare dates (handles both date and datetime types)
        sales_date_col = pd.to_datetime(sales_df['datetime__sales_date']).dt.date
        day_sales_df = sales_df[sales_date_col == day_to_process.date()]
        
        if not day_sales_df.empty:
            # Load in chunks
            for i in range(0, len(day_sales_df), CHUNK_SIZE):
                chunk = day_sales_df.iloc[i:i+CHUNK_SIZE]
                chunk.to_sql('staging_sales', target_engine, schema='dbo', if_exists='append', index=False)
                total_sales_rows += len(chunk)
        
        # --- FILTER & LOAD SALES ITEMS for this day ---
        # Convert to datetime first, then compare dates (handles both date and datetime types)
        items_date_col = pd.to_datetime(items_df['datetime__sales_date']).dt.date
        day_items_df = items_df[items_date_col == day_to_process.date()]
        
        if not day_items_df.empty:
            # Select ONLY columns that staging table expects (drop item_name, datetime__sales_date)
            staging_cols = ['id', 'item_code', 'sales_no', 'INT_QUANTITY', 'double_price', 
                           'double_total_discount_amount', 'double_mgst_tax_amount', 
                           'DOUBLE_TOTAL_TAX_AMOUNT', 'double_sub_total', 'double_cost', 'voucher_no']
            day_items_staging = day_items_df[staging_cols]
            
            # Load in chunks
            for i in range(0, len(day_items_staging), CHUNK_SIZE):
                chunk = day_items_staging.iloc[i:i+CHUNK_SIZE]
                chunk.to_sql('staging_sales_items', target_engine, schema='dbo', if_exists='append', index=False)
                total_salesitem_rows += len(chunk)

        # --- FILTER & LOAD PAYMENTS for this day ---
        # Convert to datetime first, then compare dates (handles both date and datetime types)
        payments_date_col = pd.to_datetime(payments_df['DATETIME__DATE']).dt.date
        day_payments_df = payments_df[payments_date_col == day_to_process.date()]
        
        if not day_payments_df.empty:
            # Load in chunks
            for i in range(0, len(day_payments_df), CHUNK_SIZE):
                chunk = day_payments_df.iloc[i:i+CHUNK_SIZE]
                chunk.to_sql('staging_payments', target_engine, schema='dbo', if_exists='append', index=False)
                total_payment_rows += len(chunk)

        # Dispose engine after use
        target_engine.dispose()

        duration = time.perf_counter() - start_ts
        return {
            "day": day_str,
            "status": "OK",
            "sales": total_sales_rows,
            "items": total_salesitem_rows,
            "payments": total_payment_rows,
            "duration": duration
        }
    except Exception as e:
        return {
            "day": day_str,
            "status": "FAILED",
            "error": str(e),
            "sales": total_sales_rows,
            "items": total_salesitem_rows,
            "payments": total_payment_rows,
            "duration": time.perf_counter() - start_ts
        }

def main():
    """Main ELT function for the historical sales fact data.
    
    PHASE 2: Parquet-based ETL (Offline Processing)
    - Reads from local Parquet files (no Xilnex connection needed)
    - Faster processing (local disk vs. network queries)
    - Eliminates IP firewall issues
    - Can retry unlimited times without re-extracting from source
    """
    print("Starting historical ELT for sales facts (Parquet-based)...")
    print("=" * 80)
    print(f"Export directory: {EXPORT_DIR}")
    print(f"Processing month: {MONTH_TO_PROCESS}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("=" * 80)
    
    date_range = pd.date_range(start=START_DATE, end=END_DATE)
    
    # Step 0: Clear staging tables
    print("\nStep 0: Clearing staging tables in warehouse...")
    initial_engine = get_db_engine("TARGET")
    with initial_engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE [dbo].[staging_sales]"))
        connection.execute(text("TRUNCATE TABLE [dbo].[staging_sales_items]"))
        connection.execute(text("TRUNCATE TABLE [dbo].[staging_payments]"))
        connection.execute(text("TRUNCATE TABLE [dbo].[staging_recipe_costs]"))
        connection.commit()
        print("  Staging tables cleared\n")
    initial_engine.dispose()
    
    # Step 1: Load Parquet files into memory (ONE TIME)
    print("Step 1: Loading Parquet files into memory...")
    try:
        sales_parquet = EXPORT_DIR / f"sales_{MONTH_TO_PROCESS}.parquet"
        items_parquet = EXPORT_DIR / f"sales_items_{MONTH_TO_PROCESS}.parquet"
        payments_parquet = EXPORT_DIR / f"payments_{MONTH_TO_PROCESS}.parquet"
        recipe_parquet = EXPORT_DIR / "recipe_costs.parquet"
        
        print(f"  Loading {sales_parquet}...")
        sales_df = pd.read_parquet(sales_parquet)
        print(f"    Loaded {len(sales_df):,} sales records")
        
        print(f"  Loading {items_parquet}...")
        items_df = pd.read_parquet(items_parquet)
        print(f"    Loaded {len(items_df):,} sales item records")
        
        print(f"  Loading {payments_parquet}...")
        payments_df = pd.read_parquet(payments_parquet)
        print(f"    Loaded {len(payments_df):,} payment records")
        
        print(f"  Loading {recipe_parquet}...")
        recipe_df = pd.read_parquet(recipe_parquet)
        print(f"    Loaded {len(recipe_df):,} recipe cost records\n")
        
    except FileNotFoundError as e:
        print(f"ERROR: Parquet file not found: {e}")
        print("Please run export_to_parquet.py first to create the files.")
        return
    except Exception as e:
        print(f"ERROR loading Parquet files: {e}")
        return

    # Step 2: Load recipe costs to staging (from Parquet, not database)
    print("Step 2: Loading recipe costs to staging table...")
    recipe_engine = get_db_engine("TARGET")
    if not recipe_df.empty:
        recipe_df.to_sql('staging_recipe_costs', recipe_engine, schema='dbo', if_exists='append', index=False)
        print(f"  Loaded {len(recipe_df):,} recipe cost records\n")
    else:
        print("  WARNING: No recipe costs found in Parquet file\n")
    recipe_engine.dispose()

    # Step 3: Load sales data day-by-day using parallel workers
    print(f"Step 3: Loading sales data using {MAX_WORKERS} parallel workers...")
    print(f"Processing {len(date_range)} days from {START_DATE} to {END_DATE}")
    print(f"Source: Parquet files (offline, no Xilnex connection needed!)")
    print(f"Auto-selected max workers: {MAX_WORKERS}")
    print("=" * 80 + "\n")
    
    day_results = []
    failures = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all days to the thread pool, passing pre-loaded DataFrames
        futures = [
            executor.submit(process_single_day, single_date, sales_df, items_df, payments_df) 
            for single_date in date_range
        ]
        
        # Collect results as they complete
        for future in as_completed(futures):
            result = future.result()
            day_results.append(result)
            if result["status"] == "OK":
                print(f"[OK] {result['day']} -> {result['sales']} sales, {result['items']} items, "
                      f"{result['payments']} payments in {result['duration']:.2f}s")
            else:
                failures += 1
                print(f"[FAILED] {result['day']} -> {result.get('error', 'Unknown error')}")
    
    print("\n" + "=" * 80)
    print("All Extract and Load steps completed successfully!")
    print("=" * 80)
    print(f"Total records loaded from Parquet files:")
    print(f"   Sales: {len(sales_df):,}")
    print(f"   Sales Items: {len(items_df):,}")
    print(f"   Payments: {len(payments_df):,}")
    print(f"   Recipe Costs: {len(recipe_df):,}")

    success_runs = [r for r in day_results if r["status"] == "OK"]
    if success_runs:
        avg_duration = sum(r["duration"] for r in success_runs) / len(success_runs)
        print(f"\nPer-day avg duration: {avg_duration:.2f}s across {len(success_runs)} successful days")
    if failures:
        print(f"\n⚠️  {failures} day(s) failed. Check logs above for details.")
    print("=" * 80)
    print("Next Step: Run transform_sales_facts_daily.py to process the staged data.")
    print("=" * 80)

if __name__ == "__main__":
    load_dotenv()
    main()