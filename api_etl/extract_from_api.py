"""
Extract Sales Data from Xilnex Sync API
Loads to staging_sales, staging_sales_items, staging_payments

Author: YONG WERN JIE
Date: October 29, 2025 (Updated for Cloud Deployment)
"""

import http.client
import requests
import json
import os
from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from config_api import (
    API_HOST, APP_ID, TOKEN, AUTH_LEVEL,
    TARGET_START_DATE, TARGET_END_DATE,
    MAX_API_CALLS, BATCH_SIZE
)
_api_session = None

def get_api_session():
    """Return a singleton requests.Session with keep-alive and gzip enabled."""
    global _api_session
    if _api_session is None:
        session = requests.Session()
        session.headers.update({
            'appid': APP_ID,
            'token': TOKEN,
            'auth': AUTH_LEVEL,
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip'
        })
        _api_session = session
    return _api_session


# Load environment variables for cloud deployment
load_dotenv('.env.cloud')  # Cloud warehouse (TIMEdotcom)


def get_warehouse_engine():
    """Get SQLAlchemy engine for warehouse"""
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER", "localhost")
    database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
    user = os.getenv("TARGET_USERNAME", "sa")
    password = quote_plus(os.getenv("TARGET_PASSWORD", ""))  # URL-encode password
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    
    return create_engine(connection_uri, pool_pre_ping=True)


def call_sync_api(start_timestamp=None):
    """
    Call Xilnex Sync API for one batch of sales
    
    Args:
        start_timestamp: Optional timestamp for pagination
        
    Returns:
        dict: API response with sales data
    """
    url_path = "/apps/v2/sync/sales"
    if start_timestamp:
        url_path += f"?starttimestamp={start_timestamp}"
    
    headers = {
        'Accept': 'application/json, text/json, text/html, application/*+json',
        'Content-Type': 'application/json',
        'appid': APP_ID,
        'token': TOKEN,
        'auth': AUTH_LEVEL,
    }
    
    conn = http.client.HTTPSConnection(API_HOST, timeout=60)
    conn.request("GET", url_path, headers=headers)
    res = conn.getresponse()
    
    if res.status != 200:
        error_text = res.read().decode("utf-8")
        raise Exception(f"API Error {res.status}: {error_text}")
    
    data = res.read().decode("utf-8")
    conn.close()
    
    return json.loads(data)


def extract_sales_for_period_smart(start_date, end_date, enable_early_exit=True, buffer_days=7):
    """
    OPTIMIZED: Extract sales with smart early exit when sufficient data collected
    
    Strategy:
    1. Fetch batches from API using timestamp pagination
    2. Track the latest date seen in each batch
    3. Stop when we've gone BUFFER_DAYS past target end_date AND
       seen 3 consecutive batches with no records in target range
    4. Return ALL raw data (final filtering happens during load/transform)
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        enable_early_exit: Enable smart exit (default True)
        buffer_days: Days to continue past end_date (default 7)
    
    Returns:
        list: All sales records (unfiltered - includes data before/after range)
    """
    from datetime import datetime as dt, timedelta
    
    print("="*80)
    print(f"SMART EXTRACTION FROM XILNEX API (OPTIMIZED)")
    print("="*80)
    print(f"Target Date Range: {start_date} to {end_date}")
    print(f"Max API Calls Safety Cap: {MAX_API_CALLS if MAX_API_CALLS else 'UNLIMITED'}")
    print(f"Smart Early Exit: {'ENABLED' if enable_early_exit else 'DISABLED'}")
    if enable_early_exit:
        print(f"  Buffer Days: {buffer_days} (will fetch {buffer_days} days past end date)")
    print()
    print("[STRATEGY] Smart early exit enabled:")
    print("  - Stops when data goes 7+ days past target end date")
    print("  - AND 3 consecutive batches have no records in target range")
    print("  - Minimizes API calls while ensuring complete coverage")
    print()
    
    # Parse target dates for comparison
    target_start = dt.strptime(start_date, "%Y-%m-%d")
    target_end = dt.strptime(end_date, "%Y-%m-%d")
    buffer_end = target_end + timedelta(days=buffer_days)
    
    all_sales_raw = []
    last_timestamp = None
    call_count = 0
    consecutive_out_of_range = 0
    latest_date_overall = None
    
    session = get_api_session()
    
    # STEP 1: Fetch data via timestamp pagination with smart early exit
    while True:
        # Safety cap check
        if MAX_API_CALLS and call_count >= MAX_API_CALLS:
            print(f"  [WARNING] Reached safety cap of {MAX_API_CALLS} calls")
            if latest_date_overall:
                print(f"  [WARNING] Latest date reached: {latest_date_overall.date()}")
            print(f"  [WARNING] You may need to increase MAX_API_CALLS for complete coverage")
            break
        
        call_count += 1
        print(f"[Call {call_count}] Fetching batch...")
        if last_timestamp:
            print(f"  Timestamp: {last_timestamp}")
        
        try:
            # Build URL path - timestamp pagination only
            url_path = f"/apps/v2/sync/sales?limit={BATCH_SIZE}&mode=ByDateTime"
            if last_timestamp:
                url_path += f"&starttimestamp={last_timestamp}"
            
            # Print URL on first call
            if call_count == 1:
                print(f"  [DEBUG] API URL: {url_path}")
                print()
            
            url = f"https://{API_HOST}{url_path}"
            res = session.get(url, timeout=60)
            
            if res.status_code != 200:
                error_text = res.text
                raise Exception(f"API Error {res.status_code}: {error_text}")
            
            response = res.json()
            
            if not response.get('ok'):
                print(f"  [ERROR] API returned ok=false")
                break
            
            sales_batch = response.get('data', {}).get('sales', [])
            
            if not sales_batch:
                print(f"  [COMPLETE] API returned empty batch. End of data.")
                break
            
            # Analyze batch dates for smart exit logic
            batch_dates = []
            dates_in_batch_str = []
            for sale in sales_batch:
                business_dt_str = sale.get('businessDateTime', '')
                if business_dt_str:
                    try:
                        # Parse full datetime
                        sale_dt = dt.strptime(business_dt_str, "%Y-%m-%d %H:%M:%S")
                        batch_dates.append(sale_dt)
                        dates_in_batch_str.append(sale_dt.strftime("%Y-%m-%d"))
                    except:
                        # Fallback to date string
                        sale_date = sale.get('businessDate', '')
                        if sale_date:
                            dates_in_batch_str.append(sale_date[:10])
            
            if batch_dates:
                min_date = min(batch_dates)
                max_date = max(batch_dates)
                latest_date_overall = max_date
                
                print(f"  Batch dates: {min_date.date()} to {max_date.date()}")
                print(f"  Records: {len(sales_batch)}")
                
                # Smart exit logic
                if enable_early_exit and max_date > buffer_end:
                    # We've gone past our buffer
                    in_range = [d for d in batch_dates if target_start <= d <= target_end]
                    
                    if not in_range:
                        consecutive_out_of_range += 1
                        print(f"  [CHECK] No records in target range ({consecutive_out_of_range}/3)")
                        
                        if consecutive_out_of_range >= 3:
                            print()
                            print(f"  [SMART EXIT] 3 consecutive batches beyond target range")
                            print(f"  Latest date: {max_date.date()}")
                            print(f"  Target end: {end_date} (+ {buffer_days} buffer days)")
                            print(f"  Target range fully covered! ✓")
                            break
                    else:
                        consecutive_out_of_range = 0
                        print(f"  [OK] {len(in_range)} records in target range")
            elif dates_in_batch_str:
                # Fallback if datetime parsing failed
                min_date_str = min(dates_in_batch_str)
                max_date_str = max(dates_in_batch_str)
                print(f"  Batch dates: {min_date_str} to {max_date_str}")
                print(f"  Records: {len(sales_batch)}")
            else:
                print(f"  Records: {len(sales_batch)} (no date info)")
            
            # Add ALL sales to raw list (no filtering)
            all_sales_raw.extend(sales_batch)
            print(f"  Total fetched: {len(all_sales_raw):,} sales")
            
            # Get next timestamp for pagination
            last_timestamp = response.get('data', {}).get('lastTimestamp')
            if not last_timestamp:
                print(f"  [COMPLETE] No more timestamps. Reached end of API data.")
                break
            
            print()
        
        except Exception as e:
            print(f"  [ERROR] {e}")
            import traceback
            traceback.print_exc()
            break
    
    print()
    print("="*80)
    print("SMART EXTRACTION COMPLETE")
    print(f"  Total API Calls: {call_count}")
    print(f"  Total Sales Fetched: {len(all_sales_raw):,}")
    if latest_date_overall:
        print(f"  Latest Date Reached: {latest_date_overall.date()}")
    print()
    print("[NOTE] Raw data returned (not filtered)")
    print("[NOTE] Filtering will happen during staging load/transform")
    print("="*80)
    print()
    
    return all_sales_raw


def extract_sales_for_period(start_date, end_date):
    """
    LEGACY FUNCTION: Extract all sales with client-side filtering
    
    This is the old approach kept for backward compatibility.
    For new code, use extract_sales_for_period_smart() instead.
    
    Uses ByDateTime mode with timestamp pagination, then filters client-side.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        list: Sales records filtered to specified date range
    """
    print("="*80)
    print(f"EXTRACTING DATA FROM XILNEX SYNC API (LEGACY MODE)")
    print("="*80)
    print(f"Target Date Range: {start_date} to {end_date}")
    print(f"Max API Calls Safety Cap: {MAX_API_CALLS}")
    print()
    print("[INFO] Xilnex API does NOT support date filtering parameters")
    print("[INFO] Must fetch ALL data via timestamp pagination, then filter client-side")
    print("[INFO] This may require hundreds or thousands of API calls for historical data!")
    print()
    
    all_sales_raw = []  # Store ALL sales from API (before date filtering)
    last_timestamp = None
    call_count = 0

    session = get_api_session()
    
    # STEP 1: Fetch ALL data via timestamp pagination (no early stopping)
    while True:
        if MAX_API_CALLS and call_count >= MAX_API_CALLS:
            print(f"  [WARNING] Reached safety cap of {MAX_API_CALLS} calls for this period. Stopping to avoid infinite loop.")
            break

        call_count += 1
        print(f"[Call {call_count}] Fetching batch...")
        if last_timestamp:
            print(f"  Using timestamp: {last_timestamp}")

        try:
            # Build URL path - NO date filtering (API doesn't support it)
            # Only timestamp-based pagination works
            url_path = f"/apps/v2/sync/sales?limit={BATCH_SIZE}&mode=ByDateTime"
            if last_timestamp:
                url_path += f"&starttimestamp={last_timestamp}"
            
            # DIAGNOSTIC: Print URL on first call only
            if call_count == 1:
                print(f"  [DEBUG] API URL: {url_path}")
                print()

            url = f"https://{API_HOST}{url_path}"
            res = session.get(url, timeout=60)

            if res.status_code != 200:
                error_text = res.text
                raise Exception(f"API Error {res.status_code}: {error_text}")

            response = res.json()

            if not response.get('ok'):
                print(f"  [ERROR] API returned ok=false")
                break

            sales_batch = response.get('data', {}).get('sales', [])

            if not sales_batch:
                print(f"  [COMPLETE] API returned empty batch. No more data.")
                break

            # DIAGNOSTIC: Check date range in this batch
            dates_in_batch = []
            for sale in sales_batch:
                sale_date = sale.get('businessDate', 'NO_DATE')
                if sale_date and sale_date != 'NO_DATE':
                    dates_in_batch.append(sale_date[:10])  # Get YYYY-MM-DD part
            
            if dates_in_batch:
                min_date = min(dates_in_batch)
                max_date = max(dates_in_batch)
                print(f"  [!] DATE RANGE IN BATCH: {min_date} to {max_date}")
            
            # Add ALL sales to raw list (no date filtering yet)
            all_sales_raw.extend(sales_batch)
            print(f"  Retrieved: {len(sales_batch)} sales")
            print(f"  Total fetched so far: {len(all_sales_raw)} sales")

            # Get next timestamp for pagination
            last_timestamp = response.get('data', {}).get('lastTimestamp')
            if not last_timestamp:
                print(f"  [COMPLETE] No more timestamps. Reached end of API data.")
                break

            print(f"  Next timestamp: {last_timestamp}")
            print()

        except Exception as e:
            print(f"  [ERROR] {e}")
            import traceback
            traceback.print_exc()
            break
    
    print()
    print("="*80)
    print("TIMESTAMP PAGINATION COMPLETE")
    print(f"  Total API Calls: {call_count}")
    print(f"  Total Sales Fetched: {len(all_sales_raw)}")
    print("="*80)
    print()
    
    # STEP 2: Filter by target date range AFTER fetching all data
    print("="*80)
    print("FILTERING BY DATE RANGE")
    print("="*80)
    print(f"Target: {start_date} to {end_date}")
    print()
    
    all_sales = []
    date_stats = {
        'before_start': 0,
        'in_range': 0,
        'after_end': 0,
        'invalid_date': 0
    }
    
    for sale in all_sales_raw:
        business_date_str = sale.get('businessDateTime', '')
        if not business_date_str:
            date_stats['invalid_date'] += 1
            continue
        
        try:
            business_date = business_date_str.split('T')[0]  # Extract date part
            
            if business_date < start_date:
                date_stats['before_start'] += 1
            elif business_date > end_date:
                date_stats['after_end'] += 1
            else:
                # In target range!
                all_sales.append(sale)
                date_stats['in_range'] += 1
        except Exception as e:
            date_stats['invalid_date'] += 1
            print(f"  [WARNING] Invalid date format: {business_date_str}")
    
    print(f"  Before start date ({start_date}): {date_stats['before_start']:,}")
    print(f"  In target range: {date_stats['in_range']:,} [OK]")
    print(f"  After end date ({end_date}): {date_stats['after_end']:,}")
    print(f"  Invalid dates: {date_stats['invalid_date']}")
    print()
    
    print("="*80)
    print(f"EXTRACTION COMPLETE")
    print(f"  API Calls: {call_count}")
    print(f"  Total Fetched: {len(all_sales_raw):,}")
    print(f"  Filtered (In Range): {len(all_sales):,}")
    print("="*80)
    print()
    
    return all_sales


def save_raw_json(sales, month_identifier='data'):
    """
    Save raw sales data to JSON file for reference
    
    Args:
        sales: List of sales records
        month_identifier: String to identify the month (e.g., 'October_2018')
    """
    os.makedirs('api_data', exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'api_data/raw_sales_{month_identifier}_{timestamp}.json'
    
    # Extract date range from sales if available
    if sales:
        dates = [s.get('businessDateTime', '').split('T')[0] for s in sales if s.get('businessDateTime')]
        date_range = f"{min(dates)} to {max(dates)}" if dates else "Unknown"
    else:
        date_range = "No sales"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump({
            'extraction_date': datetime.now().isoformat(),
            'sales_count': len(sales),
            'date_range': date_range,
            'sales': sales
        }, f, indent=2, default=str)
    
    print(f"[SAVED] Raw JSON: {filename}")
    print()
    
    return filename


def load_to_staging(sales):
    """
    Load sales data to staging tables using FAST batch processing
    
    Args:
        sales: List of sales records from API
    """
    print("="*80)
    print("LOADING TO STAGING TABLES (BATCH MODE - OPTIMIZED)")
    print("="*80)
    
    engine = get_warehouse_engine()
    conn = engine.connect()
    
    try:
        # Skip clearing - pandas will append (for testing speed)
        print("Skipping table clearing (appending data)...")
        print("  [OK] Ready to load")
        print()
        
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # ========== SALES HEADERS (BATCH MODE) ==========
        print("Loading sales headers...")
        sales_data = []
        for sale in sales:
            sales_data.append({
                'SaleID': sale.get('id'),
                'BusinessDateTime': sale.get('businessDateTime'),
                'SystemDateTime': sale.get('dateTime'),
                'OutletID': sale.get('outletId'),
                'OutletName': sale.get('outlet'),
                'CashierName': sale.get('cashier'),
                'SalesType': sale.get('salesType'),
                'SubSalesType': sale.get('subSalesType'),
                'GrandTotal': sale.get('grandTotal', 0.0),
                'NetAmount': sale.get('netAmount', 0.0),
                'TaxAmount': sale.get('gstTaxAmount', 0.0),
                'Paid': sale.get('paid', 0.0),
                'Balance': sale.get('balance', 0.0),
                'Rounding': sale.get('rounding', 0.0),
                'PaxNumber': int(sale.get('paxNumber', '0')) if sale.get('paxNumber') else None,
                'BillDiscountAmount': sale.get('billDiscountAmount', 0.0),
                'OrderNo': sale.get('orderNo'),
                'PaymentStatus': sale.get('paymentStatus'),
                'Status': sale.get('status'),
                'BatchID': batch_id
            })
        
        print(f"  Inserting {len(sales_data)} sales using pandas to_sql()...")
        
        # Use pandas to_sql which is much faster and more reliable
        df_sales = pd.DataFrame(sales_data)
        df_sales.to_sql('staging_sales', engine, schema='dbo', if_exists='append', 
                        index=False, chunksize=1000)
        
        print(f"  [OK] Loaded {len(sales_data)} sales headers")
        print()
        
        # ========== SALES ITEMS (BATCH MODE) ==========
        print("Loading sales items...")
        items_data = []
        for sale in sales:
            sale_id = sale.get('id')
            for item in sale.get('items', []):
                # Calculate net and total amounts
                subtotal = item.get('subtotal', 0.0) or 0.0
                discount = item.get('discountAmount', 0.0) or 0.0
                tax = item.get('totalTaxAmount', 0.0) or 0.0
                net_amount = subtotal - discount
                total_amount = net_amount + tax
                
                items_data.append({
                    'ItemID': item.get('id'),
                    'SaleID': sale_id,
                    'ProductID': item.get('itemId'),
                    'ProductCode': item.get('itemCode'),
                    'ProductName': item.get('itemName'),
                    'Category': item.get('category'),
                    'Quantity': item.get('quantity', 0.0),
                    'UnitPrice': item.get('unitPrice', 0.0),
                    'Subtotal': subtotal,
                    'DiscountAmount': discount,
                    'NetAmount': net_amount,
                    'TaxAmount': tax,
                    'TotalAmount': total_amount,
                    'TaxCode': item.get('taxCode'),
                    'TaxRate': item.get('gstPercentage'),
                    'Cost': item.get('cost', 0.0),
                    'IsFOC': 1 if item.get('foc') else 0,
                    'Model': item.get('model'),
                    'IsServiceCharge': 1 if item.get('isServiceCharge') else 0,
                    'SalesType': item.get('salesType'),
                    'SubSalesType': item.get('salesitemSubsalesType'),
                    'SalesPerson': item.get('salesPerson'),
                    'BatchID': batch_id
                })
        
        print(f"  Inserting {len(items_data)} items using pandas to_sql()...")
        
        # Use pandas to_sql
        df_items = pd.DataFrame(items_data)
        df_items.to_sql('staging_sales_items', engine, schema='dbo', if_exists='append',
                       index=False, chunksize=1000)
        
        print(f"  [OK] Loaded {len(items_data)} sales items")
        print()
        
        # ========== PAYMENTS (BATCH MODE) ==========
        print("Loading payments...")
        payments_data = []
        for sale in sales:
            sale_id = sale.get('id')
            for payment in sale.get('collection', []):
                payments_data.append({
                    'PaymentID': payment.get('id'),
                    'SaleID': sale_id,
                    'PaymentMethod': payment.get('method'),
                    'Amount': payment.get('amount', 0.0),
                    'PaymentDateTime': payment.get('paymentDate'),
                    'BusinessDate': payment.get('businessDate'),
                    'PaymentReference': payment.get('reference'),
                    'EODSessionID': payment.get('declarationSessionId'),
                    'TenderAmount': payment.get('tenderAmount'),
                    'ChangeAmount': payment.get('change'),
                    'CardType': payment.get('cardType') or payment.get('cardType2') or payment.get('cardType3'),
                    'IsVoid': 1 if payment.get('isVoid') else 0,
                    'BatchID': batch_id
                })
        
        print(f"  Inserting {len(payments_data)} payments using pandas to_sql()...")
        
        # Use pandas to_sql
        df_payments = pd.DataFrame(payments_data)
        df_payments.to_sql('staging_payments', engine, schema='dbo', if_exists='append',
                          index=False, chunksize=1000)
        
        print(f"  [OK] Loaded {len(payments_data)} payments")
        print()
        
        print("="*80)
        print("STAGING LOAD COMPLETE (PANDAS)")
        print(f"  Sales: {len(sales_data)}")
        print(f"  Items: {len(items_data)}")
        print(f"  Payments: {len(payments_data)}")
        print("="*80)
        print()
        
    except Exception as e:
        print(f"[ERROR] Failed to load staging data: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        conn.close()


def load_to_staging_upsert(sales, start_date=None, end_date=None):
    """
    OPTIMIZED: Load sales to staging with UPSERT (no duplicates)
    
    Uses pandas + SQL MERGE for proper deduplication:
    - If record exists (by SaleID/ItemID/PaymentID): UPDATE
    - If record doesn't exist: INSERT
    - Handles append mode safely (no truncate needed)
    - Filters by date range if provided
    
    Args:
        sales: List of sales records from API
        start_date: Optional - filter to this start date
        end_date: Optional - filter to this end date
    """
    from datetime import datetime as dt
    
    print("="*80)
    print("LOADING TO STAGING TABLES (OPTIMIZED UPSERT MODE)")
    print("="*80)
    if start_date and end_date:
        print(f"Date Filter: {start_date} to {end_date}")
    else:
        print("Date Filter: None (loading all data)")
    print()
    
    engine = get_warehouse_engine()
    
    # Filter sales by date range if specified
    if start_date and end_date:
        filtered_sales = []
        for sale in sales:
            business_date_str = sale.get('businessDateTime', '')
            if business_date_str:
                try:
                    business_date = business_date_str.split('T')[0]
                    if start_date <= business_date <= end_date:
                        filtered_sales.append(sale)
                except:
                    pass
        print(f"[FILTER] {len(filtered_sales):,} of {len(sales):,} sales in date range")
        sales = filtered_sales
    
    if not sales:
        print("[WARNING] No sales to load after filtering")
        return
    
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    
    try:
        # ========== PREPARE SALES HEADERS ==========
        print("[1/6] Preparing sales headers...")
        sales_data = []
        for sale in sales:
            sales_data.append({
                'SaleID': sale.get('id'),
                'BusinessDateTime': sale.get('businessDateTime'),
                'SystemDateTime': sale.get('dateTime'),
                'OutletID': sale.get('outletId'),
                'OutletName': sale.get('outlet'),
                'CashierName': sale.get('cashier'),
                'SalesType': sale.get('salesType'),
                'SubSalesType': sale.get('subSalesType'),
                'GrandTotal': sale.get('grandTotal', 0.0),
                'NetAmount': sale.get('netAmount', 0.0),
                'TaxAmount': sale.get('gstTaxAmount', 0.0),
                'Paid': sale.get('paid', 0.0),
                'Balance': sale.get('balance', 0.0),
                'Rounding': sale.get('rounding', 0.0),
                'PaxNumber': int(sale.get('paxNumber', '0')) if sale.get('paxNumber') else None,
                'BillDiscountAmount': sale.get('billDiscountAmount', 0.0),
                'OrderNo': sale.get('orderNo'),
                'PaymentStatus': sale.get('paymentStatus'),
                'Status': sale.get('status'),
                'BatchID': batch_id
            })
        
        df_sales = pd.DataFrame(sales_data)
        print(f"  Prepared {len(df_sales)} sales records")
        
        # ========== UPSERT SALES HEADERS ==========
        print("[2/6] Upserting sales headers...")
        with engine.begin() as conn:
            # Create temp table
            conn.execute(text("""
                IF OBJECT_ID('tempdb..#temp_sales') IS NOT NULL DROP TABLE #temp_sales;
                CREATE TABLE #temp_sales (
                    SaleID BIGINT PRIMARY KEY,
                    BusinessDateTime DATETIME,
                    SystemDateTime DATETIME,
                    OutletID UNIQUEIDENTIFIER,
                    OutletName NVARCHAR(255),
                    CashierName NVARCHAR(255),
                    SalesType NVARCHAR(50),
                    SubSalesType NVARCHAR(50),
                    GrandTotal DECIMAL(18,2),
                    NetAmount DECIMAL(18,2),
                    TaxAmount DECIMAL(18,2),
                    Paid DECIMAL(18,2),
                    Balance DECIMAL(18,2),
                    Rounding DECIMAL(18,2),
                    PaxNumber INT,
                    BillDiscountAmount DECIMAL(18,2),
                    OrderNo NVARCHAR(100),
                    PaymentStatus NVARCHAR(50),
                    Status NVARCHAR(50),
                    BatchID NVARCHAR(50)
                )
            """))
            
            # Bulk insert to temp
            df_sales.to_sql('#temp_sales', conn, if_exists='append', index=False)
            
            # MERGE (upsert)
            result = conn.execute(text("""
                MERGE dbo.staging_sales AS target
                USING #temp_sales AS source
                ON target.SaleID = source.SaleID
                WHEN MATCHED THEN
                    UPDATE SET
                        BusinessDateTime = source.BusinessDateTime,
                        SystemDateTime = source.SystemDateTime,
                        OutletID = source.OutletID,
                        OutletName = source.OutletName,
                        CashierName = source.CashierName,
                        SalesType = source.SalesType,
                        SubSalesType = source.SubSalesType,
                        GrandTotal = source.GrandTotal,
                        NetAmount = source.NetAmount,
                        TaxAmount = source.TaxAmount,
                        Paid = source.Paid,
                        Balance = source.Balance,
                        Rounding = source.Rounding,
                        PaxNumber = source.PaxNumber,
                        BillDiscountAmount = source.BillDiscountAmount,
                        OrderNo = source.OrderNo,
                        PaymentStatus = source.PaymentStatus,
                        Status = source.Status,
                        BatchID = source.BatchID
                WHEN NOT MATCHED THEN
                    INSERT (SaleID, BusinessDateTime, SystemDateTime, OutletID, OutletName,
                            CashierName, SalesType, SubSalesType, GrandTotal, NetAmount,
                            TaxAmount, Paid, Balance, Rounding, PaxNumber, BillDiscountAmount,
                            OrderNo, PaymentStatus, Status, BatchID)
                    VALUES (source.SaleID, source.BusinessDateTime, source.SystemDateTime,
                            source.OutletID, source.OutletName, source.CashierName,
                            source.SalesType, source.SubSalesType, source.GrandTotal,
                            source.NetAmount, source.TaxAmount, source.Paid, source.Balance,
                            source.Rounding, source.PaxNumber, source.BillDiscountAmount,
                            source.OrderNo, source.PaymentStatus, source.Status, source.BatchID);
            """))
            
            print(f"  [OK] Upserted {len(df_sales)} sales (duplicates handled)")
        
        # ========== PREPARE SALES ITEMS ==========
        print("[3/6] Preparing sales items...")
        items_data = []
        for sale in sales:
            sale_id = sale.get('id')
            for item in sale.get('items', []):
                subtotal = item.get('subtotal', 0.0) or 0.0
                discount = item.get('discountAmount', 0.0) or 0.0
                tax = item.get('totalTaxAmount', 0.0) or 0.0
                net_amount = subtotal - discount
                total_amount = net_amount + tax
                
                items_data.append({
                    'ItemID': item.get('id'),
                    'SaleID': sale_id,
                    'ProductID': item.get('itemId'),
                    'ProductCode': item.get('itemCode'),
                    'ProductName': item.get('itemName'),
                    'Category': item.get('category'),
                    'Quantity': item.get('quantity', 0.0),
                    'UnitPrice': item.get('unitPrice', 0.0),
                    'Subtotal': subtotal,
                    'DiscountAmount': discount,
                    'NetAmount': net_amount,
                    'TaxAmount': tax,
                    'TotalAmount': total_amount,
                    'TaxCode': item.get('taxCode'),
                    'TaxRate': item.get('gstPercentage'),
                    'Cost': item.get('cost', 0.0),
                    'IsFOC': 1 if item.get('foc') else 0,
                    'Model': item.get('model'),
                    'IsServiceCharge': 1 if item.get('isServiceCharge') else 0,
                    'SalesType': item.get('salesType'),
                    'SubSalesType': item.get('salesitemSubsalesType'),
                    'SalesPerson': item.get('salesPerson'),
                    'BatchID': batch_id
                })
        
        df_items = pd.DataFrame(items_data)
        print(f"  Prepared {len(df_items)} item records")
        
        # ========== UPSERT SALES ITEMS ==========
        print("[4/6] Upserting sales items...")
        with engine.begin() as conn:
            # For items, we'll delete existing items for these SaleIDs first, then insert
            # This is simpler than MERGE for line items and handles quantity changes
            sale_ids = ','.join(str(int(sid)) for sid in df_sales['SaleID'].unique())
            
            delete_result = conn.execute(text(f"""
                DELETE FROM dbo.staging_sales_items
                WHERE SaleID IN ({sale_ids})
            """))
            
            print(f"  Deleted {delete_result.rowcount if delete_result.rowcount else 0} existing items")
            
            # Insert all items
            df_items.to_sql('staging_sales_items', conn, schema='dbo', 
                           if_exists='append', index=False, chunksize=1000)
            
            print(f"  [OK] Inserted {len(df_items)} items")
        
        # ========== PREPARE PAYMENTS ==========
        print("[5/6] Preparing payments...")
        payments_data = []
        for sale in sales:
            sale_id = sale.get('id')
            for payment in sale.get('collection', []):
                payments_data.append({
                    'PaymentID': payment.get('id'),
                    'SaleID': sale_id,
                    'PaymentMethod': payment.get('method'),
                    'Amount': payment.get('amount', 0.0),
                    'PaymentDateTime': payment.get('paymentDate'),
                    'BusinessDate': payment.get('businessDate'),
                    'PaymentReference': payment.get('reference'),
                    'EODSessionID': payment.get('declarationSessionId'),
                    'TenderAmount': payment.get('tenderAmount'),
                    'ChangeAmount': payment.get('change'),
                    'CardType': payment.get('cardType') or payment.get('cardType2') or payment.get('cardType3'),
                    'IsVoid': 1 if payment.get('isVoid') else 0,
                    'BatchID': batch_id
                })
        
        df_payments = pd.DataFrame(payments_data)
        print(f"  Prepared {len(df_payments)} payment records")
        
        # ========== UPSERT PAYMENTS ==========
        print("[6/6] Upserting payments...")
        with engine.begin() as conn:
            # Same strategy as items - delete existing, then insert
            delete_result = conn.execute(text(f"""
                DELETE FROM dbo.staging_payments
                WHERE SaleID IN ({sale_ids})
            """))
            
            print(f"  Deleted {delete_result.rowcount if delete_result.rowcount else 0} existing payments")
            
            # Insert all payments
            df_payments.to_sql('staging_payments', conn, schema='dbo',
                             if_exists='append', index=False, chunksize=1000)
            
            print(f"  [OK] Inserted {len(df_payments)} payments")
        
        print()
        print("="*80)
        print("STAGING UPSERT COMPLETE")
        print(f"  Sales: {len(df_sales):,} records")
        print(f"  Items: {len(df_items):,} records")
        print(f"  Payments: {len(df_payments):,} records")
        print(f"  Batch ID: {batch_id}")
        print("="*80)
        print()
        
    except Exception as e:
        print(f"[ERROR] Failed to upsert staging data: {e}")
        import traceback
        traceback.print_exc()
        raise


def extract_october_2018():
    """
    Backward compatibility wrapper for October 2018 extraction
    Uses the default TARGET_START_DATE and TARGET_END_DATE from config_api.py
    """
    return extract_sales_for_period(TARGET_START_DATE, TARGET_END_DATE)


def main():
    """Main execution function (for backward compatibility with old scripts)"""
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║     XILNEX SYNC API - DATA EXTRACTION                         ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()
    
    # Step 1: Extract from API
    sales = extract_sales_for_period(TARGET_START_DATE, TARGET_END_DATE)
    
    if not sales:
        print("[WARNING] No sales data retrieved. Exiting.")
        return
    
    # Step 2: Save raw JSON
    save_raw_json(sales, f"{TARGET_START_DATE}_to_{TARGET_END_DATE}".replace('-', ''))
    
    # Step 3: Load to staging tables
    load_to_staging(sales)
    
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║                    EXTRACTION COMPLETE!                        ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()
    print("Next steps:")
    print("  1. Run: python api_etl/transform_api_to_facts.py")
    print("  2. Verify data in fact_sales_transactions_api")
    print("  3. Test via FastAPI endpoints")
    print()


if __name__ == "__main__":
    # Change to parent directory so imports work
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    main()

