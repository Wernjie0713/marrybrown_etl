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
    """Get SQLAlchemy engine for warehouse (using pyodbc fast_executemany directly)"""
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


def extract_sales_for_period(start_date, end_date):
    """
    Extract all sales for a specific date range via API pagination
    
    IMPORTANT: Uses timestamp-based pagination (not date-based).
    Fetches ALL data via timestamp pagination, then filters by date.
    This is required because Xilnex API returns data in timestamp order,
    not chronological business date order, so early stopping by date
    would miss data that was inserted out of order.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        list: All sales records in the specified date range
    """
    print("="*80)
    print(f"EXTRACTING DATA FROM XILNEX SYNC API")
    print("="*80)
    print(f"Target Date Range: {start_date} to {end_date}")
    print(f"Max API Calls Safety Cap: {MAX_API_CALLS}")
    print()
    print("[INFO] Fetching ALL data via timestamp pagination...")
    print("[INFO] Will filter by date AFTER fetching all batches.")
    print()
    
    all_sales_raw = []  # Store ALL sales from API (before date filtering)
    last_timestamp = None
    call_count = 0

    session = get_api_session()
    
    # STEP 1: Fetch ALL data via timestamp pagination (no early stopping)
    while True:
        if call_count >= MAX_API_CALLS:
            print(f"  [WARNING] Reached safety cap of {MAX_API_CALLS} calls for this period. Stopping to avoid infinite loop.")
            break

        call_count += 1
        print(f"[Call {call_count}] Fetching batch...")
        if last_timestamp:
            print(f"  Using timestamp: {last_timestamp}")

        try:
            # Build URL path for requests (mirror of call_sync_api)
            url_path = f"/sync/sales?limit={BATCH_SIZE}"
            if last_timestamp:
                url_path += f"&timestamp={last_timestamp}"

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
    print(f"  In target range: {date_stats['in_range']:,} ✓")
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

