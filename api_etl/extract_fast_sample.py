"""
Fast Sample Data Extraction - Optimized for Speed
Extracts sales data from Xilnex API with parallel DB writes for maximum throughput

KEY OPTIMIZATIONS:
1. Sequential API calls (required by API timestamp pagination)
2. Parallel DB writes (3 tables: sales, items, payments)
3. Simple INSERT (not MERGE - faster for sample data)
4. Larger batch accumulation (20K-30K records before writing)
5. Resume capability via api_sync_metadata (stores lastTimestamp)

CORRECT API USAGE:
- API does NOT support date range filtering
- Use starttimestamp parameter for pagination (hex value from lastTimestamp)
- First call: /apps/v2/sync/sales?limit=1000 (no starttimestamp)
- Subsequent calls: /apps/v2/sync/sales?limit=1000&starttimestamp=<lastTimestamp>
- Date filtering happens client-side (for early exit logic only)

Author: YONG WERN JIE
Date: December 2025
"""

import requests
import json
import sys
import os
import time
from typing import Optional, List, Dict
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
from urllib.parse import quote_plus
import random
import pyodbc
from decimal import Decimal, InvalidOperation
import math

# Ensure project root is on sys.path when running as a script
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config_api import (
    API_HOST,
    APP_ID,
    TOKEN,
    AUTH_LEVEL,
    BATCH_SIZE,
    MAX_API_CALLS,
)
from metadata_store import ApiSyncMetadataStore
from api_etl.transform_api_to_facts import transform_to_facts_optimized

# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env.cloud')
load_dotenv(ENV_PATH)

# Configuration
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))
API_RETRY_BASE_DELAY = float(os.getenv("API_RETRY_BASE_DELAY", "2"))
BATCH_ACCUMULATION_SIZE = max(1, int(os.getenv("FAST_SAMPLE_BATCH_SIZE", "10000")))
MAX_BUFFER_SECONDS = int(os.getenv("FAST_SAMPLE_MAX_BUFFER_SECONDS", "300"))  # Time-based flush safety net
ENABLE_EARLY_EXIT = True  # Stop when date range is exceeded
BUFFER_DAYS = 7  # Continue 7 days past end_date

# Optional test mode: limit to N batch flushes then run transform + exit
TEST_TWO_BATCHES_ONLY = os.getenv("FAST_SAMPLE_TEST_TWO_BATCHES_ONLY", "true").lower() == "true"
TEST_BATCH_LIMIT = max(1, int(os.getenv("FAST_SAMPLE_TEST_BATCH_LIMIT", "2")))

# Global session
_api_session = None


def _unwrap_pyodbc_connection(sa_connection) -> pyodbc.Connection:
    """Best-effort to obtain the underlying pyodbc connection from SQLAlchemy."""
    candidate = getattr(sa_connection, "connection", sa_connection)
    
    # SQLAlchemy 1.4/2.x keeps the DBAPI connection on .connection.driver_connection
    if hasattr(candidate, "driver_connection"):
        candidate = candidate.driver_connection
    elif hasattr(candidate, "dbapi_connection"):
        candidate = candidate.dbapi_connection
    
    # Some pools wrap the raw connection; unwrap common attributes
    if hasattr(candidate, "connection") and not isinstance(candidate, pyodbc.Connection):
        candidate = candidate.connection
    
    if not isinstance(candidate, pyodbc.Connection):
        raise RuntimeError("Expected a pyodbc connection.")
    
    return candidate


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


def get_warehouse_engine():
    """Get SQLAlchemy engine for warehouse"""
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER", "localhost")
    database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
    user = os.getenv("TARGET_USERNAME", "sa")
    password = quote_plus(os.getenv("TARGET_PASSWORD", ""))
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
        "&timeout=60"
        "&login_timeout=60"
    )
    
    engine = create_engine(
        connection_uri, 
        poolclass=NullPool,
        echo=False,
        connect_args={
            "timeout": 60,
            "login_timeout": 60
        }
    )
    
    return engine


def perform_api_call(session, url: str):
    """
    Execute an API call with retry logic.
    Returns (response, latency_seconds, retries_used).
    """
    attempt = 0
    while attempt < API_MAX_RETRIES:
        attempt += 1
        start_time = time.perf_counter()
        try:
            response = session.get(url, timeout=90)
            latency = time.perf_counter() - start_time
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ConnectionResetError) as exc:
            wait = API_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            wait += random.uniform(0.5, 1.5)
            print(f"  [RETRY {attempt}] Connection error: {exc}, waiting {wait:.1f}s")
            time.sleep(wait)
            continue

        if response.status_code == 200:
            return response, latency, attempt - 1

        if response.status_code in (429, 503):
            retry_after = response.headers.get("Retry-After")
            try:
                wait = float(retry_after)
            except (TypeError, ValueError):
                wait = API_RETRY_BASE_DELAY * (2 ** (attempt - 1))
        elif 500 <= response.status_code < 600:
            wait = API_RETRY_BASE_DELAY * (2 ** (attempt - 1))
        else:
            response.raise_for_status()
            break

        wait += random.uniform(0.25, 1.0)
        print(f"  [RETRY {attempt}] HTTP {response.status_code}, waiting {wait:.1f}s")
        time.sleep(wait)

    raise RuntimeError(f"API request {url} failed after {API_MAX_RETRIES} attempts")


def get_location_key_from_outlet(outlet_name, conn):
    """Get LocationKey from dim_locations based on outlet name. Creates new if not found."""
    if not outlet_name:
        return None
    
    result = conn.execute(text("""
        SELECT LocationKey FROM dim_locations 
        WHERE LocationName = :outlet_name
    """), {"outlet_name": outlet_name}).fetchone()
    
    if result:
        return result[0]
    
    new_location_key = conn.execute(text("""
        INSERT INTO dim_locations (LocationName, City, State)
        OUTPUT INSERTED.LocationKey
        VALUES (:outlet_name, 'Unknown', 'Unknown')
    """), {"outlet_name": outlet_name}).fetchone()[0]
    
    return new_location_key


def transform_sales_to_dataframes(sales_list: List[Dict], engine) -> tuple:
    """
    Transform sales list into 3 dataframes: sales, items, payments.
    Returns (sales_df, items_df, payments_df)
    """
    if not sales_list:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Get unique outlets and resolve LocationKeys
    unique_outlets = set()
    for sale in sales_list:
        outlet = sale.get('outlet')
        if outlet:
            unique_outlets.add(outlet)
    
    outlet_location_mapping = {}
    with engine.begin() as conn:
        for outlet_name in unique_outlets:
            location_key = get_location_key_from_outlet(outlet_name, conn)
            outlet_location_mapping[outlet_name] = location_key
    
    # Process sales
    sales_data = []
    items_data = []
    payments_data = []
    
    for sale in sales_list:
        sales_id = sale.get('id')
        
        # Prepare sales record
        sales_record = sale.copy()
        # Serialize complex fields
        for key in ['items', 'collection', 'voucher', 'extendedsales', 'billingaddress', 'shippingaddress', 'client']:
            if key in sales_record and isinstance(sales_record[key], (dict, list)):
                sales_record[key] = json.dumps(sales_record[key])
        # Add LocationKey
        sales_record['LocationKey'] = outlet_location_mapping.get(sale.get('outlet'))
        sales_data.append(sales_record)
        
        # Process items
        for item in sale.get('items', []):
            item_record = item.copy()
            # Serialize complex fields
            for key, value in item_record.items():
                if isinstance(value, (dict, list)):
                    item_record[key] = json.dumps(value)
            item_record['SaleID'] = sales_id
            items_data.append(item_record)
        
        # Process payments
        for payment in sale.get('collection', []):
            payment_record = payment.copy()
            # Serialize complex fields
            for key, value in payment_record.items():
                if isinstance(value, (dict, list)):
                    payment_record[key] = json.dumps(value)
            payment_record['SaleID'] = sales_id
            payments_data.append(payment_record)
    
    # Convert to DataFrames
    sales_df = pd.DataFrame(sales_data) if sales_data else pd.DataFrame()
    items_df = pd.DataFrame(items_data) if items_data else pd.DataFrame()
    payments_df = pd.DataFrame(payments_data) if payments_data else pd.DataFrame()
    
    return sales_df, items_df, payments_df



def normalize_text(text_val):
    """Normalize text for dimension matching (uppercase, stripped)."""
    if pd.isna(text_val) or text_val is None:
        return None
    if not isinstance(text_val, str):
        return text_val
    val = text_val.strip().upper()
    return val if val else None


def write_sales_batch(engine, sales_df: pd.DataFrame):
    """Write sales dataframe to staging_sales using executemany (same approach as extract_from_api_chunked.py)."""
    if sales_df.empty:
        return 0
    
    table_name = 'dbo.staging_sales'
    
    # Define valid columns using ORIGINAL API field names that we actually load
    # into the lean dbo.staging_sales table (no wide JSON blobs).
    valid_columns = [
        'id',
        'dateTime',
        'systemDateTime',
        'outlet',
        'cashier',
        'salesType',
        'subSalesType',
        'grandTotal',
        'netAmount',
        'paid',
        'balance',
        'rounding',
        'paxNumber',
        'billDiscountAmount',
        'orderNo',
        'paymentStatus',
        'status',
        'batchId',
        # New columns for dimension keys
        'outletId',
        'siteId',
        'clientName',
        'clientId',
        'outletCode',
        'orderSource',
        'deliveryType',
        'salesPerson'
    ]
    
    # Filter DataFrame to only valid columns that exist
    cols_to_use = [col for col in sales_df.columns if col in valid_columns]
    
    if not cols_to_use:
        raise RuntimeError(f"No matching columns found for {table_name}")
    
    df_filtered = sales_df[cols_to_use].copy()
    
    # Map API field names to staging table schema column names
    column_mapping = {
        'id': 'SaleID',
        'dateTime': 'BusinessDateTime',
        'systemDateTime': 'SystemDateTime',
        'outlet': 'OutletName',
        'cashier': 'CashierName',
        'salesType': 'SalesType',
        'subSalesType': 'SubSalesType',
        'grandTotal': 'GrandTotal',
        'netAmount': 'NetAmount',
        'paid': 'Paid',
        'balance': 'Balance',
        'rounding': 'Rounding',
        'paxNumber': 'PaxNumber',
        'billDiscountAmount': 'BillDiscountAmount',
        'orderNo': 'OrderNo',
        'paymentStatus': 'PaymentStatus',
        'status': 'Status',
        'batchId': 'BatchID',
        # Mappings for new columns
        'outletId': 'OutletID',
        'siteId': 'TerminalCode',  # Mapping siteId to TerminalCode
        'clientName': 'CustomerName',
        'clientId': 'CustomerID',
        'outletCode': 'OutletCode',
        'orderSource': 'OrderSource',
        'deliveryType': 'DeliveryType',
        'salesPerson': 'SalesPerson'
    }
    
    # Normalization for dimension matching
    if 'outlet' in df_filtered.columns:
        df_filtered['outlet'] = df_filtered['outlet'].apply(normalize_text)
    if 'cashier' in df_filtered.columns:
        df_filtered['cashier'] = df_filtered['cashier'].apply(normalize_text)
    if 'salesType' in df_filtered.columns:
        df_filtered['salesType'] = df_filtered['salesType'].apply(normalize_text)
    if 'subSalesType' in df_filtered.columns:
        df_filtered['subSalesType'] = df_filtered['subSalesType'].apply(normalize_text)
    if 'paymentStatus' in df_filtered.columns:
        df_filtered['paymentStatus'] = df_filtered['paymentStatus'].apply(normalize_text)
    if 'status' in df_filtered.columns:
        df_filtered['status'] = df_filtered['status'].apply(normalize_text)
    if 'clientName' in df_filtered.columns:
        df_filtered['clientName'] = df_filtered['clientName'].apply(normalize_text)
    if 'salesPerson' in df_filtered.columns:
        df_filtered['salesPerson'] = df_filtered['salesPerson'].apply(normalize_text)
    
    # Rename columns to match staging table schema
    mapping = {col: column_mapping[col] for col in cols_to_use if col in column_mapping}
    df_filtered = df_filtered.rename(columns=mapping)
    
    # Add LoadedAt so NOT NULL constraint/defaults are satisfied on staging table
    if 'LoadedAt' not in df_filtered.columns:
        df_filtered['LoadedAt'] = pd.Timestamp.utcnow()
    
    db_columns = list(df_filtered.columns)

    
    # Decimal precision hints (same as extract_from_api_chunked.py)
    decimal_precision_hints = {
        'GrandTotal': (18, 2),
        'NetAmount': (18, 2),
        'Paid': (18, 2),
        'Balance': (18, 2),
        'Rounding': (18, 2),
        'BillDiscountAmount': (18, 2)
    }
    
    # Get raw pyodbc connection
    with engine.begin() as conn:
        raw_conn = _unwrap_pyodbc_connection(conn)
        cursor = raw_conn.cursor()
        
        # Build SQL
        col_list = ", ".join(db_columns)
        placeholders = ", ".join(["?" for _ in db_columns])
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples with proper type handling
        rows = []
        decimal_quantizers = {
            col: (Decimal("1").scaleb(-decimal_precision_hints[col][1]) if decimal_precision_hints[col][1] and decimal_precision_hints[col][1] > 0 else Decimal("1"))
            for col in decimal_precision_hints
        }
        
        for row in df_filtered.itertuples(index=False, name=None):
            converted_row = []
            for col_idx, val in enumerate(row):
                if pd.isna(val):
                    converted_row.append(None)
                    continue
                
                col_name = db_columns[col_idx]
                
                # Handle decimal precision
                if col_name in decimal_precision_hints:
                    precision, scale = decimal_precision_hints[col_name]
                    quantizer = decimal_quantizers.get(col_name, Decimal("1"))
                    try:
                        if isinstance(val, Decimal):
                            dec_value = val
                        elif isinstance(val, (int, float)):
                            if isinstance(val, float) and math.isnan(val):
                                converted_row.append(None)
                                continue
                            dec_value = Decimal(str(val))
                        elif isinstance(val, str):
                            val = val.strip()
                            if val == "":
                                converted_row.append(None)
                                continue
                            dec_value = Decimal(val)
                        else:
                            dec_value = Decimal(str(val))
                        
                        if dec_value.is_nan() or dec_value.is_infinite():
                            converted_row.append(None)
                            continue
                        
                        if scale is not None and scale >= 0:
                            dec_value = dec_value.quantize(quantizer)
                            if scale == 0:
                                converted_row.append(f"{dec_value:.0f}")
                            else:
                                converted_row.append(f"{dec_value:.{scale}f}")
                        else:
                            converted_row.append(str(dec_value))
                        continue
                    except (InvalidOperation, ValueError, TypeError):
                        converted_row.append(None)
                        continue
                
                # Handle other types
                converted_row.append(val)
            
            rows.append(tuple(converted_row))
        
        # Execute batch insert
        try:
            cursor.executemany(sql, rows)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"  [ERROR] Failed to insert sales data: {e}")
            print(f"  [DEBUG] SQL: {sql[:200]}...")
            print(f"  [DEBUG] Columns: {db_columns}")
            raise
    
    return len(df_filtered)


def write_items_batch(engine, items_df: pd.DataFrame):
    """Write items dataframe to staging_sales_items using executemany (same approach as extract_from_api_chunked.py)."""
    if items_df.empty:
        return 0
    
    table_name = 'dbo.staging_sales_items'
    
    # Define valid columns using ORIGINAL API field names (same as extract_from_api_chunked.py)
    valid_columns = [
        'id', 'itemId', 'itemCode', 'itemName', 'category', 'quantity',
        'unitPrice', 'subtotal', 'discountAmount', 'totalTaxAmount', 'taxCode',
        'gstPercentage', 'cost', 'foc', 'model', 'isServiceCharge', 'salesType',
        'salesitemSubsalesType', 'salesPerson', 'SaleID'  # SaleID added programmatically
    ]
    
    # Filter DataFrame to only valid columns that exist
    cols_to_use = [col for col in items_df.columns if col in valid_columns]
    
    if not cols_to_use:
        raise RuntimeError(f"No matching columns found for {table_name}")
    
    df_filtered = items_df[cols_to_use].copy()
    
    # Normalize text columns
    if 'salesPerson' in df_filtered.columns:
        df_filtered['salesPerson'] = df_filtered['salesPerson'].apply(normalize_text)
    if 'salesType' in df_filtered.columns:
        df_filtered['salesType'] = df_filtered['salesType'].apply(normalize_text)
    if 'salesitemSubsalesType' in df_filtered.columns:
        df_filtered['salesitemSubsalesType'] = df_filtered['salesitemSubsalesType'].apply(normalize_text)
    if 'category' in df_filtered.columns:
        df_filtered['category'] = df_filtered['category'].apply(normalize_text)
    
    # Derive monetary fields for facts
    # Use fillna(0) to ensure calculations work
    subtotal = pd.to_numeric(df_filtered.get('subtotal', 0), errors='coerce').fillna(0)
    discount = pd.to_numeric(df_filtered.get('discountAmount', 0), errors='coerce').fillna(0)
    tax = pd.to_numeric(df_filtered.get('totalTaxAmount', 0), errors='coerce').fillna(0)
    cost = pd.to_numeric(df_filtered.get('cost', 0), errors='coerce').fillna(0)
    quantity = pd.to_numeric(df_filtered.get('quantity', 0), errors='coerce').fillna(0)
    
    # Calculate derived amounts
    # NetAmount = Subtotal - Discount
    # TotalAmount = Subtotal + Tax (assuming Subtotal is pre-tax)
    # CostAmount = Cost * Quantity (assuming Cost is unit cost)
    df_filtered['NetAmount'] = subtotal - discount
    df_filtered['TotalAmount'] = subtotal + tax
    df_filtered['CostAmount'] = cost * quantity

    # Map API field names to staging table schema column names (same as extract_from_api_chunked.py)
    column_mapping = {
        'id': 'ItemID',
        'itemId': 'ProductID',
        'itemCode': 'ProductCode',
        'itemName': 'ProductName',
        'category': 'Category',
        'quantity': 'Quantity',
        'unitPrice': 'UnitPrice',
        'subtotal': 'Subtotal',
        'discountAmount': 'DiscountAmount',
        'totalTaxAmount': 'TaxAmount',
        'taxCode': 'TaxCode',
        'gstPercentage': 'TaxRate',
        'cost': 'Cost',
        'foc': 'IsFOC',
        'model': 'Model',
        'isServiceCharge': 'IsServiceCharge',
        'salesType': 'SalesType',
        'salesitemSubsalesType': 'SubSalesType',
        'salesPerson': 'SalesPerson',
        'SaleID': 'SaleID'  # Added programmatically, maps to database column
    }
    
    # Rename columns to match staging table schema
    mapping = {col: column_mapping[col] for col in cols_to_use if col in column_mapping}
    df_filtered = df_filtered.rename(columns=mapping)
    
    # Ensure derived columns are in final DataFrame (they might not be in mapping)
    # They are already named correctly: NetAmount, TotalAmount, CostAmount
    
    db_columns = list(df_filtered.columns)
    
    # Decimal precision hints (same as extract_from_api_chunked.py)
    decimal_precision_hints = {
        'Quantity': (18, 3),
        'UnitPrice': (18, 2),
        'Subtotal': (18, 2),
        'DiscountAmount': (18, 2),
        'TaxAmount': (18, 2),
        'TaxRate': (18, 4),
        'Cost': (18, 2),
        'NetAmount': (18, 2),
        'TotalAmount': (18, 2),
        'CostAmount': (18, 2)
    }
    
    # Get raw pyodbc connection
    with engine.begin() as conn:
        raw_conn = _unwrap_pyodbc_connection(conn)
        cursor = raw_conn.cursor()
        
        # Build SQL
        col_list = ", ".join(db_columns)
        placeholders = ", ".join(["?" for _ in db_columns])
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples with proper type handling
        rows = []
        decimal_quantizers = {
            col: (Decimal("1").scaleb(-decimal_precision_hints[col][1]) if decimal_precision_hints[col][1] and decimal_precision_hints[col][1] > 0 else Decimal("1"))
            for col in decimal_precision_hints
        }
        
        for row in df_filtered.itertuples(index=False, name=None):
            converted_row = []
            for col_idx, val in enumerate(row):
                if pd.isna(val):
                    converted_row.append(None)
                    continue
                
                col_name = db_columns[col_idx]
                
                # Handle decimal precision
                if col_name in decimal_precision_hints:
                    precision, scale = decimal_precision_hints[col_name]
                    quantizer = decimal_quantizers.get(col_name, Decimal("1"))
                    try:
                        if isinstance(val, Decimal):
                            dec_value = val
                        elif isinstance(val, (int, float)):
                            if isinstance(val, float) and math.isnan(val):
                                converted_row.append(None)
                                continue
                            dec_value = Decimal(str(val))
                        elif isinstance(val, str):
                            val = val.strip()
                            if val == "":
                                converted_row.append(None)
                                continue
                            dec_value = Decimal(val)
                        else:
                            dec_value = Decimal(str(val))
                        
                        if dec_value.is_nan() or dec_value.is_infinite():
                            converted_row.append(None)
                            continue
                        
                        if scale is not None and scale >= 0:
                            dec_value = dec_value.quantize(quantizer)
                            if scale == 0:
                                converted_row.append(f"{dec_value:.0f}")
                            else:
                                converted_row.append(f"{dec_value:.{scale}f}")
                        else:
                            converted_row.append(str(dec_value))
                        continue
                    except (InvalidOperation, ValueError, TypeError):
                        converted_row.append(None)
                        continue
                
                # Handle other types
                converted_row.append(val)
            
            rows.append(tuple(converted_row))
        
        # Execute batch insert
        try:
            cursor.executemany(sql, rows)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"  [ERROR] Failed to insert items data: {e}")
            print(f"  [DEBUG] SQL: {sql[:200]}...")
            print(f"  [DEBUG] Columns: {db_columns}")
            raise
    
    return len(df_filtered)


def write_payments_batch(engine, payments_df: pd.DataFrame):
    """Write payments dataframe to staging_payments using executemany (same approach as extract_from_api_chunked.py)."""
    if payments_df.empty:
        return 0
    
    table_name = 'dbo.staging_payments'
    
    # Define valid columns using ORIGINAL API field names (same as extract_from_api_chunked.py)
    valid_columns = [
        'id', 'method', 'amount', 'paymentDate', 'businessDate', 'reference',
        'declarationSessionId', 'tenderAmount', 'change', 'cardType', 'isVoid', 'SaleID'  # SaleID added programmatically
    ]
    
    # Filter DataFrame to only valid columns that exist
    cols_to_use = [col for col in payments_df.columns if col in valid_columns]
    
    if not cols_to_use:
        raise RuntimeError(f"No matching columns found for {table_name}")
    
    df_filtered = payments_df[cols_to_use].copy()
    
    # Normalize payment method and card type for matching
    if 'method' in df_filtered.columns:
        df_filtered['method'] = df_filtered['method'].apply(normalize_text)
    if 'cardType' in df_filtered.columns:
        df_filtered['cardType'] = df_filtered['cardType'].apply(normalize_text)
    
    # Map API field names to staging table schema column names (same as extract_from_api_chunked.py)
    column_mapping = {
        'id': 'PaymentID',
        'method': 'PaymentMethod',
        'amount': 'Amount',
        'paymentDate': 'PaymentDateTime',
        'businessDate': 'BusinessDate',
        'reference': 'PaymentReference',
        'declarationSessionId': 'EODSessionID',
        'tenderAmount': 'TenderAmount',
        'change': 'ChangeAmount',
        'cardType': 'CardType',
        'isVoid': 'IsVoid',
        'SaleID': 'SaleID'  # Added programmatically, maps to database column
    }
    
    # Rename columns to match staging table schema
    mapping = {col: column_mapping[col] for col in cols_to_use if col in column_mapping}
    df_filtered = df_filtered.rename(columns=mapping)
    db_columns = list(df_filtered.columns)
    
    # Decimal precision hints (same as extract_from_api_chunked.py)
    decimal_precision_hints = {
        'Amount': (18, 2),
        'TenderAmount': (18, 2),
        'ChangeAmount': (18, 2)
    }
    
    # Get raw pyodbc connection
    with engine.begin() as conn:
        raw_conn = _unwrap_pyodbc_connection(conn)
        cursor = raw_conn.cursor()
        
        # Build SQL
        col_list = ", ".join(db_columns)
        placeholders = ", ".join(["?" for _ in db_columns])
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples with proper type handling
        rows = []
        decimal_quantizers = {
            col: (Decimal("1").scaleb(-decimal_precision_hints[col][1]) if decimal_precision_hints[col][1] and decimal_precision_hints[col][1] > 0 else Decimal("1"))
            for col in decimal_precision_hints
        }
        
        for row in df_filtered.itertuples(index=False, name=None):
            converted_row = []
            for col_idx, val in enumerate(row):
                if pd.isna(val):
                    converted_row.append(None)
                    continue
                
                col_name = db_columns[col_idx]
                
                # Handle decimal precision
                if col_name in decimal_precision_hints:
                    precision, scale = decimal_precision_hints[col_name]
                    quantizer = decimal_quantizers.get(col_name, Decimal("1"))
                    try:
                        if isinstance(val, Decimal):
                            dec_value = val
                        elif isinstance(val, (int, float)):
                            if isinstance(val, float) and math.isnan(val):
                                converted_row.append(None)
                                continue
                            dec_value = Decimal(str(val))
                        elif isinstance(val, str):
                            val = val.strip()
                            if val == "":
                                converted_row.append(None)
                                continue
                            dec_value = Decimal(val)
                        else:
                            dec_value = Decimal(str(val))
                        
                        if dec_value.is_nan() or dec_value.is_infinite():
                            converted_row.append(None)
                            continue
                        
                        if scale is not None and scale >= 0:
                            dec_value = dec_value.quantize(quantizer)
                            if scale == 0:
                                converted_row.append(f"{dec_value:.0f}")
                            else:
                                converted_row.append(f"{dec_value:.{scale}f}")
                        else:
                            converted_row.append(str(dec_value))
                        continue
                    except (InvalidOperation, ValueError, TypeError):
                        converted_row.append(None)
                        continue
                
                # Handle other types
                converted_row.append(val)
            
            rows.append(tuple(converted_row))
        
        # Execute batch insert
        try:
            cursor.executemany(sql, rows)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"  [ERROR] Failed to insert payments data: {e}")
            print(f"  [DEBUG] SQL: {sql[:200]}...")
            print(f"  [DEBUG] Columns: {db_columns}")
            raise
    
    return len(df_filtered)


def write_parallel(engine, sales_df: pd.DataFrame, items_df: pd.DataFrame, payments_df: pd.DataFrame):
    """
    Write all three dataframes to database in parallel using ThreadPoolExecutor.
    This is the key optimization - parallel DB writes.
    """
    start_time = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(write_sales_batch, engine, sales_df): 'sales',
            executor.submit(write_items_batch, engine, items_df): 'items',
            executor.submit(write_payments_batch, engine, payments_df): 'payments',
        }
        
        results = {}
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                count = future.result()
                results[table_name] = count
            except Exception as e:
                print(f"  [ERROR] Failed to write {table_name}: {e}")
                results[table_name] = 0
    
    duration = time.perf_counter() - start_time
    print(f"  [PARALLEL WRITE] {results.get('sales', 0):,} sales, {results.get('items', 0):,} items, {results.get('payments', 0):,} payments in {duration:.1f}s")
    
    return results


def parse_sale_datetime(sale: dict) -> Optional[datetime]:
    """Parse a sale's datetime from API response."""
    candidates = [
        ("dateTime", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),
        ("businessDateTime", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),
        ("salesDate", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),
    ]
    for key, fmts in candidates:
        value = sale.get(key)
        if not value:
            continue
        for fmt in fmts:
            try:
                return datetime.strptime(value, fmt)
            except Exception:
                continue
    return None


def extract_fast_sample(
    start_date: str,
    end_date: str,
    max_calls: Optional[int] = None,
    resume: bool = True,
    run_transform: bool = True,
):
    """
    Fast extraction with parallel DB writes and resume capability.
    
    Args:
        start_date: Start date in YYYY-MM-DD format (for client-side filtering only)
        end_date: End date in YYYY-MM-DD format (for client-side filtering only)
        max_calls: Maximum API calls (None for unlimited)
        resume: If True, resume from lastTimestamp in api_sync_metadata
        run_transform: If True, run transform_to_facts_optimized for the same
                       [start_date, end_date] window after extraction completes
    
    Returns:
        dict: Statistics about extraction
    """
    print()
    print("="*80)
    print(" "*25 + "FAST SAMPLE DATA EXTRACTION")
    print("="*80)
    print(f"  Target Date Range: {start_date} to {end_date} (client-side filtering)")
    print(f"  Batch Accumulation: {BATCH_ACCUMULATION_SIZE:,} records")
    print(f"  Time Flush: {'DISABLED' if MAX_BUFFER_SECONDS <= 0 else f'every ≤{MAX_BUFFER_SECONDS}s'}")
    print(f"  Parallel DB Writes: ENABLED (3 tables)")
    print(f"  Max API Calls: {max_calls if max_calls else 'UNLIMITED'}")
    print(f"  Early Exit: {'ENABLED' if ENABLE_EARLY_EXIT else 'DISABLED'}")
    print(f"  Resume Mode: {'ENABLED' if resume else 'DISABLED'}")
    print()
    
    # Parse dates (for client-side filtering only)
    target_start = datetime.strptime(start_date, "%Y-%m-%d")
    target_end = datetime.strptime(end_date, "%Y-%m-%d")
    buffer_end = target_end + timedelta(days=BUFFER_DAYS)
    
    # Initialize
    session = get_api_session()
    engine = get_warehouse_engine()
    metadata_store = ApiSyncMetadataStore(lambda: engine)
    
    # Job name for metadata tracking
    job_name = f"fast_extraction:{start_date}:{end_date}"
    
    # Check for resume capability
    last_timestamp = None
    if resume:
        metadata_state = metadata_store.get_state(job_name)
        if metadata_state and metadata_state.last_timestamp:
            last_timestamp = metadata_state.last_timestamp
            print(f"[RESUME] Found previous run with lastTimestamp: {last_timestamp}")
            print(f"  Records previously extracted: {metadata_state.records_extracted or 0:,}")
            print(f"  Status: {metadata_state.status or 'UNKNOWN'}")
            print()
        else:
            print("[START] No previous run found, starting from beginning...")
            print()
    else:
        print("[START] Resume disabled, starting from beginning...")
        print()
    
    # Ensure job exists in metadata
    metadata_store.ensure_job(job_name, start_date=start_date, end_date=end_date)
    
    accumulated_sales = []
    call_count = 0
    consecutive_out_of_range = 0
    latest_date_overall = None
    
    total_stats = {"sales": 0, "items": 0, "payments": 0, "api_calls": 0, "batches_written": 0}

    last_write_time = time.perf_counter()

    def flush_accumulated(reason: str):
        nonlocal accumulated_sales, last_write_time
        if not accumulated_sales:
            return False

        print()
        print(f"[BATCH WRITE] ({reason}) {len(accumulated_sales):,} records -> DB...")
        print("  [PIPELINE] Stage = STAGING_WRITE (materializing DataFrames)")
        sales_df, items_df, payments_df = transform_sales_to_dataframes(accumulated_sales, engine)
        print("  [PIPELINE] Stage = STAGING_WRITE (writing to staging tables)")
        results = write_parallel(engine, sales_df, items_df, payments_df)
        print("  [PIPELINE] Stage = STAGING_WRITE (complete)")

        total_stats["sales"] += results.get('sales', 0)
        total_stats["items"] += results.get('items', 0)
        total_stats["payments"] += results.get('payments', 0)
        total_stats["batches_written"] += 1

        metadata_store.update_checkpoint(
            job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='IN_PROGRESS',
            date_range_start=start_date,
            date_range_end=end_date,
        )

        accumulated_sales = []
        last_write_time = time.perf_counter()

        print(f"  [PROGRESS] Total: {total_stats['sales']:,} sales, {total_stats['items']:,} items, {total_stats['payments']:,} payments")
        print(f"  [CHECKPOINT] Saved lastTimestamp: {last_timestamp}")
        print()

        if TEST_TWO_BATCHES_ONLY and total_stats["batches_written"] >= TEST_BATCH_LIMIT:
            print(f"[TEST MODE] Reached batch limit ({TEST_BATCH_LIMIT}); preparing to end extraction early.")
            return True
    
        return False
    start_time = time.perf_counter()
    
    try:
        while True:
            # Check max calls limit
            if max_calls and call_count >= max_calls:
                print(f"  [SAFETY CAP] Reached {max_calls} calls")
                break
            
            call_count += 1
            
            # Build API URL (CORRECT: no mode parameter, no date parameters)
            # API only supports starttimestamp for pagination
            # Use lastTimestamp exactly as returned from API (including '0x' prefix)
            url_path = f"/apps/v2/sync/sales?limit={BATCH_SIZE}"
            if last_timestamp:
                url_path += f"&starttimestamp={last_timestamp}"
            url = f"https://{API_HOST}{url_path}"
            
            # Make API call
            try:
                print(f"[Call {call_count}] Fetching batch...", end=" ", flush=True)
                res, latency, retries = perform_api_call(session, url)
                print(f"✓ ({latency:.1f}s)")
            except RuntimeError as api_err:
                print(f"  [FATAL] {api_err}")
                break
            
            # Parse response
            try:
                response = res.json()
            except ValueError as json_err:
                print(f"  [ERROR] Failed to parse API response: {json_err}")
                break
            
            if not response.get('ok'):
                print(f"  [COMPLETE] API returned ok=false, end of data")
                break
            
            sales_batch = response.get('data', {}).get('sales', [])
            if not sales_batch:
                print(f"  [COMPLETE] API returned empty batch")
                break
            
            # Get next timestamp (use exactly as returned - includes '0x' prefix)
            last_timestamp = response.get('data', {}).get('lastTimestamp')
            if not last_timestamp:
                print(f"  [COMPLETE] No more timestamps available")
                break
            
            # Filter by date range if early exit enabled
            if ENABLE_EARLY_EXIT:
                batch_dates = [dt for sale in sales_batch if (dt := parse_sale_datetime(sale))]
                if batch_dates:
                    max_date = max(batch_dates)
                    latest_date_overall = max_date
                    if max_date > buffer_end:
                        in_range = [d for d in batch_dates if target_start <= d <= target_end]
                        if not in_range:
                            consecutive_out_of_range += 1
                            if consecutive_out_of_range >= 3:
                                print()
                                print(f"[SMART EXIT] 3 consecutive batches beyond target range")
                                print(f"  Latest date: {max_date.date()}")
                                print(f"  Target end: {end_date} (+ {BUFFER_DAYS} buffer)")
                                accumulated_sales.extend(sales_batch)
                                break
                        else:
                            consecutive_out_of_range = 0
            
            # Accumulate batch
            accumulated_sales.extend(sales_batch)
            
            should_flush_by_size = len(accumulated_sales) >= BATCH_ACCUMULATION_SIZE
            should_flush_by_time = (
                MAX_BUFFER_SECONDS > 0
                and accumulated_sales
                and (time.perf_counter() - last_write_time) >= MAX_BUFFER_SECONDS
            )

            if should_flush_by_size:
                if flush_accumulated("size threshold"):
                    break
            elif should_flush_by_time:
                if flush_accumulated("time threshold"):
                    break
        
        # Write remaining accumulated data
        if accumulated_sales:
            flush_accumulated("final flush")
        
        total_stats["api_calls"] = call_count
        
        # Final checkpoint update
        metadata_store.update_checkpoint(
            job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='COMPLETED',
            date_range_start=start_date,
            date_range_end=end_date,
        )

        # Optionally run transformation to fact table for the same window
        if run_transform:
            print()
            print("="*80)
            print("STARTING FACT TABLE TRANSFORMATION FOR EXTRACTED WINDOW")
            print("="*80)
            print("  Processing ALL staging data (timestamp-based)")
            print()
            print("  [PIPELINE] Stage = FACT_TRANSFORM (staging ➜ fact)")
            transform_to_facts_optimized()
            print("  [PIPELINE] Stage = FACT_TRANSFORM (complete)")
            print()
            print("[SUCCESS] Fact table transformation complete for extracted window.")

        # Final summary
        duration = time.perf_counter() - start_time
        print()
        print("="*80)
        print(" "*25 + "EXTRACTION COMPLETE")
        print("="*80)
        print(f"  Total API Calls: {call_count}")
        print(f"  Total Batches Written: {total_stats['batches_written']}")
        print(f"  Sales Loaded: {total_stats['sales']:,}")
        print(f"  Items Loaded: {total_stats['items']:,}")
        print(f"  Payments Loaded: {total_stats['payments']:,}")
        print(f"  Total Duration: {duration/60:.1f} minutes ({duration:.1f} seconds)")
        print(f"  Average API Call Time: {duration/call_count:.1f}s" if call_count > 0 else "")
        if latest_date_overall:
            print(f"  Latest Date Reached: {latest_date_overall.date()}")
        print()
        print("[SUCCESS] Sample data extraction complete!")
        print("="*80)
        print()
        
        return total_stats
        
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] Extraction stopped by user")
        print(f"  Progress: {total_stats['sales']:,} sales in {total_stats['batches_written']} batches")
        
        # Write remaining data if any
        if accumulated_sales:
            flush_accumulated("interrupt flush")
        
        # Update checkpoint even on interrupt
        metadata_store.update_checkpoint(
            job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='INTERRUPTED',
            date_range_start=start_date,
            date_range_end=end_date,
        )
        print(f"  [CHECKPOINT] Saved lastTimestamp: {last_timestamp} (resume available)")
        
        return total_stats


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        max_calls = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else None
    else:
        # Default: 1 year of data
        start_date = "2024-01-01"
        end_date = "2024-12-31"
        max_calls = None  # Unlimited
    
    print(f"Starting fast extraction: {start_date} to {end_date}")
    stats = extract_fast_sample(start_date, end_date, max_calls)
    print(f"\nFinal Stats: {stats}")

