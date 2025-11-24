"""
Extract Sales Data from Xilnex Sync API - CHUNKED APPROACH
Extracts in batches and loads to staging incrementally for safety

KEY IMPROVEMENTS:
1. Loads to warehouse every CHUNK_SIZE API calls (default: 50 calls = 50K records)
2. Memory efficient - clears accumulated data after each chunk save
3. Progress preservation - if crash, most data already saved
4. Early failure detection - problems detected quickly, not after 800 calls
5. Resume capability - can continue from last saved chunk

Author: YONG WERN JIE
Date: November 7, 2025
"""

import requests
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError
from http.client import IncompleteRead
import json
import sys
import os
import uuid
import random
import time
from typing import Optional
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import math
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
from urllib.parse import quote_plus
import pyodbc
from config_api import (
    API_HOST,
    APP_ID,
    TOKEN,
    AUTH_LEVEL,
    BATCH_SIZE,
    MAX_API_CALLS,
)
from monitoring import DataQualityValidator, MetricsEmitter, DataQualityError, MetricTags
from api_etl.metadata_store import ApiSyncMetadataStore
from api_etl.chunk_controller import AdaptiveChunkController, ChunkTuningConfig

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


def batch_insert_dataframe(conn, df: pd.DataFrame, table_name: str) -> None:
    """Load a dataframe into SQL Server using cursor.executemany()."""
    if df.empty:
        print(f"    [BATCH] Skipping {table_name}: empty DataFrame")
        return

    start = datetime.now()
    print(f"    [BATCH] Loading {len(df):,} rows into {table_name}...")

    # CRITICAL: Dispose of connection pool to ensure fresh schema is read
    # This fixes schema caching issues when columns are modified
    try:
        conn.engine.dispose()
    except:
        pass

    # Get raw pyodbc connection from SQLAlchemy engine
    raw_conn = _unwrap_pyodbc_connection(conn)

    cursor = raw_conn.cursor()

    # Define valid columns for each staging table (using ORIGINAL API field names)
    # These are the field names as they come from the Xilnex API response
    # NOTE: SaleID is added programmatically for items and payments, so include it in valid_columns
    valid_columns = {
        'dbo.staging_sales': [
            'id', 'dateTime', 'systemDateTime', 'outlet', 'cashier', 'salesType', 'subSalesType',
            'grandTotal', 'netAmount', 'paid', 'balance', 'rounding', 'paxNumber',
            'billDiscountAmount', 'orderNo', 'paymentStatus', 'status', 'batchId',
            'items', 'collection', 'voucher', 'extendedsales', 'billingaddress',
            'shippingaddress', 'client'
        ],
        'dbo.staging_sales_items': [
            'id', 'itemId', 'itemCode', 'itemName', 'category', 'quantity',
            'unitPrice', 'subtotal', 'discountAmount', 'totalTaxAmount', 'taxCode',
            'gstPercentage', 'cost', 'foc', 'model', 'isServiceCharge', 'salesType',
            'salesitemSubsalesType', 'salesPerson', 'SaleID'  # SaleID added programmatically
        ],
        'dbo.staging_payments': [
            'id', 'method', 'amount', 'paymentDate', 'businessDate', 'reference',
            'declarationSessionId', 'tenderAmount', 'change', 'cardType', 'isVoid', 'SaleID'  # SaleID added programmatically
        ]
    }

    # Filter DataFrame to only valid columns that exist (exact match, no case conversion)
    cols_to_use = [col for col in df.columns if col in valid_columns.get(table_name, [])]
    
    if not cols_to_use:
        print(f"    [DEBUG] DataFrame columns: {list(df.columns)}")
        print(f"    [DEBUG] Valid columns for {table_name}: {valid_columns.get(table_name, [])}")
        raise RuntimeError(f"No matching columns found for {table_name}")
    
    df_filtered = df[cols_to_use]
    
    # Map API field names to staging table schema column names
    # This is the ONLY mapping - no double-mapping
    column_mapping = {
        'dbo.staging_sales': {
            'id': 'SaleID',
            'dateTime': 'BusinessDateTime',  # Main timestamp
            'systemDateTime': 'SystemDateTime',  # System timestamp (if present in API)
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
            # Complex fields - will be serialized to JSON
            'items': 'Items',
            'collection': 'Collection',
            'voucher': 'Voucher',
            'extendedsales': 'ExtendedSales',
            'billingaddress': 'BillingAddress',
            'shippingaddress': 'ShippingAddress',
            'client': 'Client'
        },
        'dbo.staging_sales_items': {
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
        },
        'dbo.staging_payments': {
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
    }

    decimal_precision_hints = {
        'dbo.staging_sales': {
            'GrandTotal': (18, 2),
            'NetAmount': (18, 2),
            'TaxAmount': (18, 2),
            'Paid': (18, 2),
            'Balance': (18, 2),
            'Rounding': (18, 2),
            'BillDiscountAmount': (18, 2)
        },
        'dbo.staging_sales_items': {
            'Quantity': (18, 3),
            'UnitPrice': (18, 2),
            'Subtotal': (18, 2),
            'DiscountAmount': (18, 2),
            'NetAmount': (18, 2),
            'TaxAmount': (18, 2),
            'TotalAmount': (18, 2),
            'TaxRate': (18, 4),
            'Cost': (18, 2)
        },
        'dbo.staging_payments': {
            'Amount': (18, 2),
            'TenderAmount': (18, 2),
            'ChangeAmount': (18, 2)
        }
    }
    
    # Rename columns to match staging table schema
    mapping = column_mapping.get(table_name, {})
    cols_to_rename = {col: mapping[col] for col in cols_to_use if col in mapping}
    df_filtered = df_filtered.rename(columns=cols_to_rename)
    cols_to_use = list(cols_to_rename.values())
    
    # Build column list and placeholders
    col_list = ", ".join(cols_to_use)
    placeholders = ", ".join(["?" for _ in cols_to_use])
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
    
    print(f"    [DEBUG] SQL: {sql[:150]}...")
    print(f"    [DEBUG] FULL COLUMN LIST ({len(cols_to_use)} columns):")
    for i, col in enumerate(cols_to_use):
        print(f"      {i+1}. {col}")

    # Convert DataFrame to list of tuples, handling NaN/None
    # NO TRUNCATION - Database schema has been updated to support full API data
    rows = []
    problematic_columns = {} if ENABLE_VERBOSE_DEBUG else None  # Track columns with large values only when verbose
    
    table_decimal_hints = decimal_precision_hints.get(table_name, {})
    decimal_quantizers = {
        col: (Decimal("1").scaleb(-table_decimal_hints[col][1]) if table_decimal_hints[col][1] and table_decimal_hints[col][1] > 0 else Decimal("1"))
        for col in table_decimal_hints
    }
    decimal_conversion_errors = []
    
    for row_idx, row in enumerate(df_filtered.itertuples(index=False, name=None)):
        converted_row = []
        for col_idx, val in enumerate(row):
            if pd.isna(val):
                converted_row.append(None)
                continue

            col_name = cols_to_use[col_idx]

            if col_name in table_decimal_hints:
                precision, scale = table_decimal_hints[col_name]
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
                        decimal_conversion_errors.append((col_name, val, "NaN/Infinite not supported"))
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
                except (InvalidOperation, ValueError, TypeError) as err:
                    decimal_conversion_errors.append((col_name, val, str(err)))
                    # Fall through to append raw value so we don't lose data

            else:
                # DEBUG: Check for values that might be too long
                if ENABLE_VERBOSE_DEBUG and isinstance(val, str) and len(val) > 46:
                    if col_name not in problematic_columns:
                        problematic_columns[col_name] = []
                    problematic_columns[col_name].append({
                        'row': row_idx,
                        'length': len(val),
                        'value': val[:100]
                    })
                converted_row.append(val)
        rows.append(tuple(converted_row))

    if decimal_conversion_errors:
        unique_cols = sorted({col for col, _, _ in decimal_conversion_errors})
        print(f"    [DEBUG] Decimal coercion fallback for columns: {', '.join(unique_cols)} (showing first 3 issues)")
        for col_name, raw_val, err in decimal_conversion_errors[:3]:
            print(f"      [DEBUG] {col_name}: value={raw_val!r} error={err}")

    # Print all problematic columns found
    if ENABLE_VERBOSE_DEBUG and problematic_columns:
        print(f"    [DEBUG] Found {len(problematic_columns)} columns with values > 46 chars:")
        for col_name, issues in problematic_columns.items():
            print(f"      - {col_name}: {len(issues)} rows with long values")
            print(f"        First occurrence: Row {issues[0]['row']}, length={issues[0]['length']}")
            print(f"        Value preview: {issues[0]['value'][:80]}...")

    print(f"    [DEBUG] Total rows to insert: {len(rows)}")
    if ENABLE_VERBOSE_DEBUG:
    print(f"    [DEBUG] First row preview:")
    
    # Write detailed debug info to file
        # Create debug directory if it doesn't exist
        debug_dir = os.path.join(os.getcwd(), 'debug')
        os.makedirs(debug_dir, exist_ok=True)
        debug_file_path = os.path.join(debug_dir, 'etl_debug.txt')
        debug_file = open(debug_file_path, 'w', encoding='utf-8')
    debug_file.write(f"COLUMNS BEING INSERTED ({len(cols_to_use)} total):\n")
    debug_file.write("=" * 80 + "\n")
    for i, col in enumerate(cols_to_use):
        debug_file.write(f"{i+1}. {col}\n")
    debug_file.write("\n" + "=" * 80 + "\n")
    debug_file.write("FIRST ROW DATA:\n")
    debug_file.write("=" * 80 + "\n")
    
    if rows:
        for i, (col, val) in enumerate(zip(cols_to_use, rows[0])):
            if val is None:
                msg = f"{i+1}. {col}: NULL"
                print(f"      {msg}")
                debug_file.write(msg + "\n")
            elif isinstance(val, str):
                msg = f"{i+1}. {col}: str(len={len(val)}) = {val[:100]}"
                print(f"      {msg}...")
                debug_file.write(msg + "\n")
            else:
                msg = f"{i+1}. {col}: {type(val).__name__} = {val}"
                print(f"      {msg}")
                debug_file.write(msg + "\n")
    
    debug_file.close()
        print(f"    [DEBUG] Full debug output written to {debug_file_path}")
    
    # Use executemany for batch insert
    try:
        cursor.executemany(sql, rows)
        cursor.commit()
    except pyodbc.Error as e:
        print(f"    [ERROR] Failed to insert data: {e}")
        print(f"    [DEBUG] SQL: {sql}")
        print(f"    [DEBUG] Columns being inserted: {cols_to_use}")
        print(f"    [DEBUG] Number of columns: {len(cols_to_use)}")
        print(f"    [DEBUG] Number of values in first row: {len(rows[0]) if rows else 0}")
        print(f"    [DEBUG] First row data types: {[type(v).__name__ for v in rows[0]] if rows else 'No rows'}")
        if ENABLE_VERBOSE_DEBUG and problematic_columns:
        print(f"    [DEBUG] Problematic columns summary: {problematic_columns}")
        raise

    elapsed = (datetime.now() - start).total_seconds()
    print(f"    [BATCH] Loaded {len(df):,} rows into {table_name} in {elapsed:.2f}s")


def batch_merge_dataframe(conn, df: pd.DataFrame, table_name: str) -> None:
    """
    Load a dataframe into SQL Server using MERGE (upsert) for idempotent loading.
    This prevents duplicates when rerunning the ETL.
    """
    if df.empty:
        print(f"    [MERGE] Skipping {table_name}: empty DataFrame")
        return

    start = datetime.now()
    print(f"    [MERGE] Upserting {len(df):,} rows into {table_name}...")

    # Get raw pyodbc connection from SQLAlchemy engine
    raw_conn = _unwrap_pyodbc_connection(conn)
    cursor = raw_conn.cursor()

    # Define valid columns and column mappings (same as batch_insert_dataframe)
    valid_columns = {
        'dbo.staging_sales': [
            'id', 'dateTime', 'systemDateTime', 'outlet', 'cashier', 'salesType', 'subSalesType',
            'grandTotal', 'netAmount', 'paid', 'balance', 'rounding', 'paxNumber',
            'billDiscountAmount', 'orderNo', 'paymentStatus', 'status', 'batchId',
            'items', 'collection', 'voucher', 'extendedsales', 'billingaddress',
            'shippingaddress', 'client', 'LocationKey'
        ],
        'dbo.staging_sales_items': [
            'id', 'itemId', 'itemCode', 'itemName', 'category', 'quantity',
            'unitPrice', 'subtotal', 'discountAmount', 'totalTaxAmount', 'taxCode',
            'gstPercentage', 'cost', 'foc', 'model', 'isServiceCharge', 'salesType',
            'salesitemSubsalesType', 'salesPerson', 'SaleID'
        ],
        'dbo.staging_payments': [
            'id', 'method', 'amount', 'paymentDate', 'businessDate', 'reference',
            'declarationSessionId', 'tenderAmount', 'change', 'cardType', 'isVoid', 'SaleID'
        ]
    }

    column_mapping = {
        'dbo.staging_sales': {
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
            'items': 'Items',
            'collection': 'Collection',
            'voucher': 'Voucher',
            'extendedsales': 'ExtendedSales',
            'billingaddress': 'BillingAddress',
            'shippingaddress': 'ShippingAddress',
            'client': 'Client',
            'LocationKey': 'LocationKey'
        },
        'dbo.staging_sales_items': {
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
            'SaleID': 'SaleID'
        },
        'dbo.staging_payments': {
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
            'SaleID': 'SaleID'
        }
    }

    decimal_precision_hints = {
        'dbo.staging_sales': {
            'GrandTotal': (18, 2),
            'NetAmount': (18, 2),
            'TaxAmount': (18, 2),
            'Paid': (18, 2),
            'Balance': (18, 2),
            'Rounding': (18, 2),
            'BillDiscountAmount': (18, 2)
        },
        'dbo.staging_sales_items': {
            'Quantity': (18, 3),
            'UnitPrice': (18, 2),
            'Subtotal': (18, 2),
            'DiscountAmount': (18, 2),
            'NetAmount': (18, 2),
            'TaxAmount': (18, 2),
            'TotalAmount': (18, 2),
            'TaxRate': (18, 4),
            'Cost': (18, 2)
        },
        'dbo.staging_payments': {
            'Amount': (18, 2),
            'TenderAmount': (18, 2),
            'ChangeAmount': (18, 2)
        }
    }

    # Filter DataFrame to only valid columns
    cols_to_use = [col for col in df.columns if col in valid_columns.get(table_name, [])]
    
    if not cols_to_use:
        print(f"    [DEBUG] DataFrame columns: {list(df.columns)}")
        print(f"    [DEBUG] Valid columns for {table_name}: {valid_columns.get(table_name, [])}")
        raise RuntimeError(f"No matching columns found for {table_name}")
    
    df_filtered = df[cols_to_use].copy()
    
    # Rename columns to match staging table schema
    mapping = column_mapping.get(table_name, {})
    cols_to_rename = {col: mapping[col] for col in cols_to_use if col in mapping}
    df_filtered = df_filtered.rename(columns=cols_to_rename)
    db_columns = list(cols_to_rename.values())

    # Define merge keys for each table
    merge_keys = {
        'dbo.staging_sales': ['SaleID'],
        'dbo.staging_sales_items': ['SaleID', 'ItemID'],
        'dbo.staging_payments': ['SaleID', 'PaymentID']
    }
    
    merge_key_cols = merge_keys.get(table_name, [])
    if not all(col in db_columns for col in merge_key_cols):
        raise RuntimeError(f"Merge key columns {merge_key_cols} not found in DataFrame columns {db_columns}")

    # Build MERGE statement
    # ON clause (match condition)
    on_clause = " AND ".join([f"target.{col} = source.{col}" for col in merge_key_cols])
    
    # UPDATE clause (all non-key columns)
    update_cols = [col for col in db_columns if col not in merge_key_cols]
    update_set = ", ".join([f"{col} = source.{col}" for col in update_cols])
    
    # INSERT clause
    insert_cols = ", ".join(db_columns)
    insert_values = ", ".join([f"source.{col}" for col in db_columns])
    
    merge_sql = f"""
        MERGE {table_name} AS target
        USING (VALUES ({', '.join(['?' for _ in db_columns])})) AS source ({', '.join(db_columns)})
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values});
    """

    # Convert DataFrame to list of tuples with proper type conversion
    table_decimal_hints = decimal_precision_hints.get(table_name, {})
    decimal_quantizers = {
        col: (Decimal("1").scaleb(-table_decimal_hints[col][1]) if table_decimal_hints[col][1] and table_decimal_hints[col][1] > 0 else Decimal("1"))
        for col in table_decimal_hints
    }
    
    rows = []
    for row_idx, row in enumerate(df_filtered.itertuples(index=False, name=None)):
        converted_row = []
        for col_idx, val in enumerate(row):
            if pd.isna(val):
                converted_row.append(None)
                continue

            col_name = db_columns[col_idx]

            # Handle decimal precision
            if col_name in table_decimal_hints:
                precision, scale = table_decimal_hints[col_name]
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
                        dec_value = Decimal(val)
                    else:
                        dec_value = Decimal(str(val))
                    
                    # Quantize to proper scale
                    dec_value = dec_value.quantize(quantizer)
                    converted_row.append(dec_value)
                    continue
                except (InvalidOperation, ValueError, TypeError):
                    converted_row.append(None)
                    continue

            # Handle other types
            if isinstance(val, (dict, list)):
                converted_row.append(json.dumps(val))
            elif isinstance(val, bool):
                converted_row.append(1 if val else 0)
            elif isinstance(val, pd.Timestamp):
                converted_row.append(val.to_pydatetime())
            else:
                converted_row.append(val)
        
        rows.append(tuple(converted_row))

    # Execute MERGE using temporary table for batch processing
    # This is more efficient than row-by-row MERGE
    batch_size = 1000
    total_rows = len(rows)
    
    try:
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            if not batch:
                continue
            
            # Create temporary table with same structure (all NVARCHAR for flexibility)
            temp_table_name = f"#temp_{table_name.replace('.', '_').replace('dbo_', '')}_{i}"
            
            # Build CREATE TABLE statement
            create_temp_sql = f"""
                CREATE TABLE {temp_table_name} (
                    {', '.join([f'[{col}] NVARCHAR(MAX)' for col in db_columns])}
                );
            """
            cursor.execute(create_temp_sql)
            
            # Convert batch values to strings for temp table insertion
            string_batch = []
            for row in batch:
                string_row = []
                for val in row:
                    if val is None:
                        string_row.append(None)
                    elif isinstance(val, (Decimal, int, float)):
                        string_row.append(str(val))
                    elif isinstance(val, datetime):
                        string_row.append(val.strftime('%Y-%m-%d %H:%M:%S.%f'))
                    elif isinstance(val, bool):
                        string_row.append('1' if val else '0')
                    else:
                        string_row.append(str(val))
                string_batch.append(tuple(string_row))
            
            # Insert batch into temp table
            insert_temp_sql = f"""
                INSERT INTO {temp_table_name} ({', '.join([f'[{col}]' for col in db_columns])})
                VALUES ({', '.join(['?' for _ in db_columns])})
            """
            cursor.executemany(insert_temp_sql, string_batch)
            
            # Perform MERGE from temp table with type conversions
            type_conversions = {}
            for col in db_columns:
                if col in table_decimal_hints:
                    precision, scale = table_decimal_hints[col]
                    type_conversions[col] = f"TRY_CAST(source.[{col}] AS DECIMAL({precision},{scale}))"
                elif col in ['IsFOC', 'IsServiceCharge', 'IsVoid']:
                    type_conversions[col] = f"TRY_CAST(source.[{col}] AS BIT)"
                elif 'DateTime' in col or 'Date' in col:
                    type_conversions[col] = f"TRY_CAST(source.[{col}] AS DATETIME)"
                elif col == 'PaxNumber' or col == 'LocationKey':
                    type_conversions[col] = f"TRY_CAST(source.[{col}] AS INT)"
                else:
                    type_conversions[col] = f"source.[{col}]"
            
            # Build MERGE with type conversions
            merge_on_clause = " AND ".join([f"target.[{col}] = {type_conversions.get(col, f'source.[{col}]')}" for col in merge_key_cols])
            merge_update_set = ", ".join([f"[{col}] = {type_conversions.get(col, f'source.[{col}]')}" for col in update_cols])
            merge_insert_cols = ", ".join([f"[{col}]" for col in db_columns])
            merge_insert_values = ", ".join([type_conversions.get(col, f"source.[{col}]") for col in db_columns])
            
            batch_merge_sql = f"""
                MERGE {table_name} AS target
                USING {temp_table_name} AS source
                ON {merge_on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {merge_update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({merge_insert_cols})
                    VALUES ({merge_insert_values});
                
                DROP TABLE {temp_table_name};
            """
            
            cursor.execute(batch_merge_sql)
        
        cursor.commit()
    except pyodbc.Error as e:
        print(f"    [ERROR] Failed to merge data: {e}")
        print(f"    [DEBUG] SQL: {merge_sql[:200]}...")
        print(f"    [DEBUG] Columns: {db_columns}")
        print(f"    [DEBUG] Merge keys: {merge_key_cols}")
        raise

    elapsed = (datetime.now() - start).total_seconds()
    print(f"    [MERGE] Upserted {len(df):,} rows into {table_name} in {elapsed:.2f}s")


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


# Load environment variables - use .env.local for local development
# Note: This will be loaded by the calling script, but we keep it here for standalone execution
import sys
from pathlib import Path
# Add parent directory to path for utils import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from utils.env_loader import load_environment
    load_environment(force_local=True)
except ImportError:
    # Fallback if utils not available
    from dotenv import load_dotenv
    repo_root = Path(__file__).resolve().parent.parent
    local_env = repo_root / '.env.local'
    if local_env.exists():
        load_dotenv(local_env)
    else:
        cloud_env = repo_root / '.env.cloud'
        if cloud_env.exists():
            load_dotenv(cloud_env)

ENABLE_VERBOSE_DEBUG = os.getenv("ENABLE_VERBOSE_DEBUG", "false").lower() == "true"
CHUNK_MIN_SIZE = int(os.getenv("CHUNK_MIN_SIZE", "25"))
CHUNK_MAX_SIZE = int(os.getenv("CHUNK_MAX_SIZE", "125"))
CHUNK_TARGET_SECONDS = float(os.getenv("CHUNK_TARGET_SECONDS", "180"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))
API_RETRY_BASE_DELAY = float(os.getenv("API_RETRY_BASE_DELAY", "2"))
STAGING_RETENTION_DAYS = int(os.getenv("STAGING_RETENTION_DAYS", "14"))

def serialize_complex_fields(data):
    """
    Recursively serialize all dictionaries and lists to JSON strings
    for SQL Server compatibility.
    """
    if isinstance(data, dict):
        return json.dumps(data)
    elif isinstance(data, list):
        return json.dumps(data)
    else:
        return data

def get_location_key_from_outlet(outlet_name, conn):
    """
    Get LocationKey from dim_locations based on outlet name.
    Creates new location entry if not found.
    """
    if not outlet_name:
        return None
    
    # Try to find existing location
    result = conn.execute(text("""
        SELECT LocationKey FROM dim_locations 
        WHERE LocationName = :outlet_name
    """), {"outlet_name": outlet_name}).fetchone()
    
    if result:
        return result[0]
    
    # Create new location if not found
    new_location_key = conn.execute(text("""
        INSERT INTO dim_locations (LocationName, City, State)
        OUTPUT INSERTED.LocationKey
        VALUES (:outlet_name, 'Unknown', 'Unknown')
    """), {"outlet_name": outlet_name}).fetchone()[0]
    
    print(f"    [LOCATION] Created new location: {outlet_name} -> Key: {new_location_key}")
    return new_location_key

def get_warehouse_engine():
    """Get SQLAlchemy engine for warehouse"""
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER", "localhost")
    database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
    user = os.getenv("TARGET_USERNAME", "sa")
    password = quote_plus(os.getenv("TARGET_PASSWORD", ""))
    
    # Add timeout parameters for slow VPN connections
    # timeout: Connection timeout in seconds
    # login_timeout: Login timeout in seconds
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
        "&timeout=60"           # Connection timeout: 60 seconds
        "&login_timeout=60"      # Login timeout: 60 seconds
    )
    
    # Create engine with fresh connections (no connection pool caching)
    # This ensures schema changes are picked up immediately
    engine = create_engine(
        connection_uri, 
        poolclass=NullPool,  # Disable connection pooling to avoid schema caching
        echo=False,
        connect_args={
            "timeout": 60,           # Connection timeout
            "login_timeout": 60      # Login timeout
        }
    )
    
    print(f"[DEBUG] Warehouse connection target: server={server}, database={database}, user={user}")
    
    return engine


_metadata_store: Optional[ApiSyncMetadataStore] = None
_metrics_emitters: dict[str, MetricsEmitter] = {}
_quality_validator: Optional[DataQualityValidator] = None


def get_metadata_store() -> ApiSyncMetadataStore:
    global _metadata_store
    if _metadata_store is None:
        _metadata_store = ApiSyncMetadataStore(get_warehouse_engine)
    return _metadata_store


def get_metrics_emitter(job_name: str) -> MetricsEmitter:
    emitter = _metrics_emitters.get(job_name)
    if emitter is None:
        emitter = MetricsEmitter(tags=MetricTags(job_name=job_name))
        _metrics_emitters[job_name] = emitter
    return emitter


def get_quality_validator() -> DataQualityValidator:
    global _quality_validator
    if _quality_validator is None:
        _quality_validator = DataQualityValidator(get_warehouse_engine)
    return _quality_validator


def get_progress(job_id='sales_extraction'):
    """
    Get saved progress for a job
    
    Returns:
        dict: Progress data or None if not found
    """
    engine = get_warehouse_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT last_timestamp, last_call_count, start_date, end_date, 
                       chunk_size, total_sales_loaded, total_items_loaded, 
                       total_payments_loaded, status, last_updated
                FROM dbo.etl_progress 
                WHERE job_id = :job_id
            """), {"job_id": job_id})
            
            row = result.fetchone()
            if row:
                return {
                    "last_timestamp": row[0],
                    "last_call_count": row[1],
                    "start_date": row[2],
                    "end_date": row[3],
                    "chunk_size": row[4],
                    "total_sales_loaded": row[5] or 0,
                    "total_items_loaded": row[6] or 0,
                    "total_payments_loaded": row[7] or 0,
                    "status": row[8],
                    "last_updated": row[9]
                }
            return None
    except Exception as e:
        print(f"  [WARNING] Could not read progress: {e}")
        return None


def save_progress(job_id, last_timestamp, call_count, start_date, end_date, 
                  chunk_size, total_stats, status='IN_PROGRESS', error_msg=None):
    """
    Save extraction progress to database
    
    Args:
        job_id: Job identifier
        last_timestamp: Last API timestamp received
        call_count: Total API calls made
        start_date: Extraction start date
        end_date: Extraction end date
        chunk_size: Chunk size used
        total_stats: Stats dict with sales/items/payments counts
        status: Job status (IN_PROGRESS, COMPLETED, ERROR)
        error_msg: Error message if any
    """
    engine = get_warehouse_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                MERGE dbo.etl_progress AS target
                USING (SELECT :job_id AS job_id) AS source
                ON target.job_id = source.job_id
                WHEN MATCHED THEN
                    UPDATE SET
                        last_timestamp = :last_timestamp,
                        last_call_count = :call_count,
                        start_date = :start_date,
                        end_date = :end_date,
                        chunk_size = :chunk_size,
                        total_sales_loaded = :total_sales,
                        total_items_loaded = :total_items,
                        total_payments_loaded = :total_payments,
                        status = :status,
                        error_message = :error_msg,
                        last_updated = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (job_id, last_timestamp, last_call_count, start_date, end_date,
                            chunk_size, total_sales_loaded, total_items_loaded, 
                            total_payments_loaded, status, error_message, last_updated)
                    VALUES (:job_id, :last_timestamp, :call_count, :start_date, :end_date,
                            :chunk_size, :total_sales, :total_items, :total_payments, 
                            :status, :error_msg, GETDATE());
            """), {
                "job_id": job_id,
                "last_timestamp": last_timestamp,
                "call_count": call_count,
                "start_date": start_date,
                "end_date": end_date,
                "chunk_size": chunk_size,
                "total_sales": total_stats["sales"],
                "total_items": total_stats["items"],
                "total_payments": total_stats["payments"],
                "status": status,
                "error_msg": error_msg
            })
    except Exception as e:
        print(f"  [WARNING] Could not save progress: {e}")


def clear_progress(job_id='sales_extraction'):
    """
    Clear saved progress for a job (force restart from beginning)
    
    Args:
        job_id: Job identifier
    """
    engine = get_warehouse_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE dbo.etl_progress 
                SET last_timestamp = NULL,
                    last_call_count = NULL,
                    start_date = NULL,
                    end_date = NULL,
                    chunk_size = NULL,
                    total_sales_loaded = 0,
                    total_items_loaded = 0,
                    total_payments_loaded = 0,
                    status = 'READY',
                    error_message = NULL,
                    last_updated = GETDATE()
                WHERE job_id = :job_id
            """), {"job_id": job_id})
        print(f"  [RESET] Progress cleared for job: {job_id}")
    except Exception as e:
        print(f"  [WARNING] Could not clear progress: {e}")


def reset_etl_storage(clear_facts: bool = False, job_id: str = 'sales_extraction'):
    """
    Hard reset: truncate staging tables, optionally clear facts, and reset progress.
    Use before a clean re-run.
    """
    engine = get_warehouse_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE dbo.staging_payments"))
            conn.execute(text("TRUNCATE TABLE dbo.staging_sales_items"))
            conn.execute(text("TRUNCATE TABLE dbo.staging_sales"))
            if clear_facts:
                conn.execute(text("TRUNCATE TABLE dbo.fact_sales_transactions"))
        clear_progress(job_id)
        print("  [RESET] Truncated staging tables" + (" and fact table" if clear_facts else ""))
    except Exception as e:
        print(f"  [WARNING] Could not reset storage: {e}")

def load_chunk_to_staging_upsert(sales_chunk, chunk_number, start_date, end_date, debug_mode=False):
    """
    Load chunk of sales to staging tables using MERGE (idempotent)
    
    Args:
        sales_chunk: List of sales records from API
        chunk_number: Chunk number for logging
        start_date: Filter start date
        end_date: Filter end date
        debug_mode: If True, print sample dates for debugging
    
    Returns:
        dict: Statistics of loaded records
    """
    from datetime import datetime as dt
    
    if not sales_chunk:
        print(f"  [CHUNK {chunk_number}] No data to load")
        return {"sales": 0, "items": 0, "payments": 0}
    
    print(f"  [CHUNK {chunk_number}] Loading {len(sales_chunk):,} sales to staging...")
    
    # Filter by date range (inclusive of entire end day) with robust date parsing
    target_start = dt.strptime(start_date, "%Y-%m-%d")
    target_end_exclusive = dt.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    
    def parse_sale_datetime(sale: dict):
        """
        Parse a sale's datetime using actual API field names.
        Updated based on real API response analysis:
        
        API Structure:
        - Main sales record: "dateTime" (ISO format: "2018-10-01T10:28:20.000Z")
        - Items records: "businessDateTime" (ISO format: "2018-10-01T00:00:00.000Z")
        - Sales ID: "id" (not "salesId")
        
        Preference order:
          1. dateTime (main sales record timestamp)
          2. businessDateTime (from items, fallback)
          3. salesDate (from items, fallback)
        
        Returns a datetime or None if nothing parsable.
        """
        candidates = [
            ("dateTime", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),  # Main sales record
            ("businessDateTime", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),  # Items
            ("salesDate", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),  # Items
        ]
        for key, fmts in candidates:
            value = sale.get(key)
            if not value:
                continue
            for fmt in fmts:
                try:
                    parsed = dt.strptime(value, fmt)
                    return parsed
                except Exception:
                    continue
        return None
    
    # Debug: Show sample dates from API
    if debug_mode and len(sales_chunk) > 0:
        print(f"  [DEBUG] Sample dates from API:")
        sample_dates = []
        sample_fields = []
        for i, sale in enumerate(sales_chunk[:5]):  # First 5 records
            # Check what date fields are actually available
            available_fields = [key for key in sale.keys() if any(word in key.lower() for word in ['date', 'time'])]
            sample_fields.append(available_fields)
            
            # Try to parse with new logic
            sample_dt = parse_sale_datetime(sale)
            if sample_dt:
                sample_dates.append(sample_dt.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                sample_dates.append("NO_DATE")
        
        print(f"    Records 1-5: {sample_dates}")
        print(f"    Date fields found: {sample_fields[0] if sample_fields else 'None'}")
    
    filtered_sales = []
    for sale in sales_chunk:
        sale_dt = parse_sale_datetime(sale)
        if sale_dt and (target_start <= sale_dt < target_end_exclusive):
            filtered_sales.append(sale)
    
    print(f"  [CHUNK {chunk_number}] Filtered {len(filtered_sales):,}/{len(sales_chunk):,} sales in date range")
    
    if not filtered_sales:
        print(f"  [CHUNK {chunk_number}] No records in target date range, skipping")
        return {"sales": 0, "items": 0, "payments": 0}
    
    engine = get_warehouse_engine()
    
    try:
        # Fast pandas-based processing with vectorized operations
        print(f"  [CHUNK {chunk_number}] Processing {len(filtered_sales):,} sales with pandas optimization...")
        
        # Convert to DataFrame first (much faster than Python loops)
        sales_df = pd.DataFrame(filtered_sales)
        
        # Vectorized serialization - apply to all complex fields at once
        for col in sales_df.columns:
            # Check if any values in column are complex types
            sample_values = sales_df[col].dropna().head(100)
            if any(isinstance(val, (dict, list)) for val in sample_values):
                print(f"    [SERIALIZE] Processing complex field: {col}")
                sales_df[col] = sales_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        
        # NO field mapping here - batch_insert_dataframe will handle all mapping
        # This avoids double-mapping and column name conflicts
        print(f"  [CHUNK {chunk_number}] Complex field serialization completed")
        
        # Fast items processing with pandas optimization
        print(f"  [CHUNK {chunk_number}] Processing items with pandas optimization...")
        items_list = []
        for sale in filtered_sales:
            sales_id = sale.get('id')
            for item in sale.get('items', []):
                processed_item = item.copy()
                # Only serialize complex fields (dict/list)
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        processed_item[key] = json.dumps(value)
                # Add SaleID using API field name (will be mapped to SaleID in batch_insert_dataframe)
                processed_item['SaleID'] = sales_id  # Use SaleID as a special marker for batch_insert
                items_list.append(processed_item)
        
        items_df = pd.DataFrame(items_list) if items_list else pd.DataFrame()
        print(f"  [CHUNK {chunk_number}] Processed {len(items_df):,} items")
        
        # Fast payments processing with pandas optimization
        print(f"  [CHUNK {chunk_number}] Processing payments with pandas optimization...")
        payments_list = []
        for sale in filtered_sales:
            sales_id = sale.get('id')
            for payment in sale.get('collection', []):
                processed_payment = payment.copy()
                # Only serialize complex fields (dict/list)
                for key, value in payment.items():
                    if isinstance(value, (dict, list)):
                        processed_payment[key] = json.dumps(value)
                # Add SaleID using API field name (will be mapped to SaleID in batch_insert_dataframe)
                processed_payment['SaleID'] = sales_id  # Use SaleID as a special marker for batch_insert
                payments_list.append(processed_payment)
        
        payments_df = pd.DataFrame(payments_list) if payments_list else pd.DataFrame()
        print(f"  [CHUNK {chunk_number}] Processed {len(payments_df):,} payments")
        with engine.begin() as conn:
            # Resolve LocationKeys for all outlets
            # API provides 'outlet' (outlet name) -> look up in dim_locations.LocationName -> get LocationKey
            unique_outlets = sales_df['outlet'].dropna().unique()
            outlet_location_mapping = {}

            for outlet_name in unique_outlets:
                location_key = get_location_key_from_outlet(outlet_name, conn)
                outlet_location_mapping[outlet_name] = location_key
            
            # Update DataFrame with LocationKeys (map outlet name to LocationKey)
            sales_df['LocationKey'] = sales_df['outlet'].map(outlet_location_mapping)
            
            # MERGE sales (idempotent upsert)
            if not sales_df.empty:
                batch_merge_dataframe(conn, sales_df, 'dbo.staging_sales')
            
            # MERGE items (idempotent upsert)
            if not items_df.empty:
                # Add proper ItemID if missing
                if 'id' not in items_df.columns:
                    items_df['id'] = range(1, len(items_df) + 1)
                
                # Add missing fields with defaults
                items_df['costOfGoods'] = 0.0  # Default cost
                items_df['taxRate'] = 0.0      # Default tax rate
                items_df['netAmount'] = items_df.get('subtotal', 0.0)  # Map subtotal to netAmount
                items_df['totalAmount'] = items_df.get('subtotal', 0.0)  # Map subtotal to totalAmount
                items_df['batchId'] = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"  # Generated batch ID
                items_df['subSalesType'] = None  # Not available in API
                items_df['isFOC'] = 0  # Default to not free of charge
                items_df['taxAmount'] = 0.0  # Default tax amount

                batch_merge_dataframe(conn, items_df, 'dbo.staging_sales_items')
            
            # MERGE payments (idempotent upsert)
            if not payments_df.empty:
                # Add proper PaymentID if missing
                if 'id' not in payments_df.columns:
                    payments_df['id'] = range(1, len(payments_df) + 1)
                
                # Add missing fields with defaults
                payments_df['paymentDateTime'] = payments_df.get('dateTime', datetime.now())  # Use dateTime or current
                payments_df['businessDate'] = datetime.now().date()  # Current date
                payments_df['eodSessionId'] = None  # Not available in API
                payments_df['tenderAmount'] = payments_df.get('amount', 0.0)  # Use amount
                payments_df['changeAmount'] = 0.0  # Default change
                payments_df['cardType'] = None  # Not available in API
                payments_df['isVoid'] = 0  # Default to not void
                payments_df['batchId'] = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"  # Generated batch ID
                
                batch_merge_dataframe(conn, payments_df, 'dbo.staging_payments')
        
        stats = {
            "sales": len(sales_df),
            "items": len(items_df),
            "payments": len(payments_df)
        }
        
        print(f"  [CHUNK {chunk_number}] [OK] Loaded: {stats['sales']} sales, {stats['items']} items, {stats['payments']} payments")
        return stats
        
    except Exception as e:
        print(f"  [CHUNK {chunk_number}] [ERROR] loading to staging: {e}")
        raise


def perform_api_call(session, url: str, metrics: MetricsEmitter):
    """
    Execute an API call with rate-limit awareness and exponential backoff.
    Handles network errors including incomplete reads and chunked encoding errors.
    Returns (response, latency_seconds, retries_used).
    """
    attempt = 0
    while attempt < API_MAX_RETRIES:
        attempt += 1
        start_time = time.perf_counter()
        try:
            response = session.get(url, timeout=90)
            # Read content immediately to catch ChunkedEncodingError/IncompleteRead early
            # This ensures we detect connection issues before processing the response
            # The content is cached, so subsequent .json() calls will still work
            _ = response.content
            latency = time.perf_counter() - start_time
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout, 
                ConnectionResetError,
                ChunkedEncodingError,
                ProtocolError,
                IncompleteRead) as exc:
            wait = API_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            wait += random.uniform(0.5, 1.5)
            error_type = type(exc).__name__
            metrics.emit_retry_event(attempt=attempt, wait_seconds=wait, reason=f"{error_type}: {str(exc)[:100]}")
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
        metrics.emit_retry_event(
            attempt=attempt,
            wait_seconds=wait,
            reason=f"HTTP {response.status_code}",
        )
        time.sleep(wait)

    raise RuntimeError(f"API request {url} failed after {API_MAX_RETRIES} attempts")


def extract_and_load_chunked(start_date, end_date, chunk_size=50, enable_early_exit=True, 
                             buffer_days=7, resume=True, force_restart=False):
    """
    Extract from API and load to staging in chunks for safety and efficiency
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        chunk_size: Number of API calls per chunk (default 50 = 50K records)
        enable_early_exit: Enable smart exit logic (default True)
        buffer_days: Days to continue past end_date (default 7)
        resume: Enable resume from last checkpoint (default True)
        force_restart: Force restart from beginning, clearing saved progress (default False)
    
    Returns:
        dict: Statistics about extraction and loading
    """
    progress_job_id = 'sales_extraction'
    metadata_job_name = f"{progress_job_id}:{start_date}:{end_date}"
    metadata_store = get_metadata_store()
    metadata_store.ensure_job(metadata_job_name, start_date=start_date, end_date=end_date)
    metrics_emitter = get_metrics_emitter(metadata_job_name)
    quality_validator = get_quality_validator()
    chunk_controller = AdaptiveChunkController(
        initial_size=chunk_size,
        config=ChunkTuningConfig(
            min_size=CHUNK_MIN_SIZE,
            max_size=CHUNK_MAX_SIZE,
            target_duration_seconds=CHUNK_TARGET_SECONDS,
        ),
    )
    
    # Handle force restart
    if force_restart:
        print()
        print("="*80)
        print(" "*25 + "FORCE RESTART MODE")
        print("="*80)
        clear_progress(progress_job_id)
        metadata_store.update_checkpoint(
            metadata_job_name,
            last_timestamp=None,
            records_extracted=0,
            status='READY',
            date_range_start=start_date,
            date_range_end=end_date,
            chunk_number=None,
            chunk_row_count=None,
            chunk_duration_seconds=None,
            chunk_completed_at=None,
        )
        print()
    
    saved_progress = None
    metadata_state = metadata_store.get_state(metadata_job_name)
    if resume and not force_restart:
        saved_progress = get_progress(progress_job_id)
        if saved_progress and saved_progress.get('last_timestamp'):
            print()
            print("="*80)
            print(" "*25 + "RESUME MODE DETECTED")
            print("="*80)
            print(f"  Last Run: {saved_progress['last_updated']}")
            print(f"  Last Status: {saved_progress['status']}")
            print(f"  API Calls Completed: {saved_progress['last_call_count']}")
            print(f"  Sales Already Loaded: {saved_progress['total_sales_loaded']:,}")
            print(f"  Items Already Loaded: {saved_progress['total_items_loaded']:,}")
            print(f"  Payments Already Loaded: {saved_progress['total_payments_loaded']:,}")
            if metadata_state and metadata_state.last_timestamp:
                print(f"  Metadata Resume Timestamp: {metadata_state.last_timestamp}")
            print()
            print("[RESUME] Continuing from last saved checkpoint...")
            print("="*80)
            print()
    
    print()
    print("="*80)
    print(" "*20 + "CHUNKED EXTRACTION & LOADING")
    print("="*80)
    print(f"  Target Date Range: {start_date} to {end_date}")
    print(f"  Chunk Size: start={chunk_controller.current_chunk_size} "
          f"(min={CHUNK_MIN_SIZE}, max={CHUNK_MAX_SIZE}) API calls")
    print(f"  Max API Calls: {MAX_API_CALLS if MAX_API_CALLS else 'UNLIMITED'}")
    print(f"  Smart Early Exit: {'ENABLED' if enable_early_exit else 'DISABLED'}")
    print(f"  Resume Mode: {'ENABLED' if resume else 'DISABLED'}")
    if enable_early_exit:
        print(f"  Buffer Days: {buffer_days}")
    print()
    print("  Strategy:")
    print("    - Adaptive chunk controller keeps checkpoints ~3 minutes each")
    print("    - Each chunk loads to staging, validates, emits metrics, persists resume point")
    print("    - Automatic retry/backoff + metadata checkpoints make the run crash-safe")
    print()
    
    # Parse dates
    target_start = datetime.strptime(start_date, "%Y-%m-%d")
    target_end = datetime.strptime(end_date, "%Y-%m-%d")
    buffer_end = target_end + timedelta(days=buffer_days)
    
    # Initialize tracking variables
    accumulated_sales = []
    call_count = 0
    chunk_count = metadata_state.last_chunk_number if metadata_state and metadata_state.last_chunk_number else 0
    consecutive_out_of_range = 0
    latest_date_overall = None
    
    total_stats = {"sales": 0, "items": 0, "payments": 0, "api_calls": 0}
    last_timestamp = None
    
    if saved_progress and resume and not force_restart:
        call_count = saved_progress.get('last_call_count') or 0
        total_stats['sales'] = saved_progress.get('total_sales_loaded', 0)
        total_stats['items'] = saved_progress.get('total_items_loaded', 0)
        total_stats['payments'] = saved_progress.get('total_payments_loaded', 0)
        last_timestamp = (
            metadata_state.last_timestamp
            if metadata_state and metadata_state.last_timestamp
            else saved_progress.get('last_timestamp')
        )
        print(f"[RESUME] Starting from API call #{call_count + 1} (Chunk #{chunk_count + 1})")
        print()
    elif metadata_state and metadata_state.last_timestamp:
        last_timestamp = metadata_state.last_timestamp
        print(f"[RESUME] Metadata indicates last timestamp {last_timestamp}. Continuing...")
        print()
    else:
        print("[START] Beginning fresh extraction from the beginning...")
        print()
    
    chunk_controller.start_chunk_window()
    calls_in_current_chunk = 0
    retries_in_chunk = 0
    total_retries = 0

    def flush_chunk(reason_label: str) -> None:
        nonlocal accumulated_sales, chunk_count, calls_in_current_chunk, retries_in_chunk, total_stats
        if not accumulated_sales:
            print(f"[{reason_label}] No accumulated sales to flush.")
            return
        chunk_number = chunk_count + 1
        print()
        print(f"{'='*80}")
        print(f"[{reason_label} {chunk_number}] Saving chunk after {call_count} API calls...")
        print(f"{'='*80}")
        chunk_duration = chunk_controller.chunk_duration()
        try:
            stats = load_chunk_to_staging_upsert(accumulated_sales, chunk_number, start_date, end_date, debug_mode=True)
            total_stats["sales"] += stats["sales"]
            total_stats["items"] += stats["items"]
            total_stats["payments"] += stats["payments"]

            dq = quality_validator.validate_staging_chunk(
                start_date=start_date,
                end_date=end_date,
                expected_sales=stats["sales"],
            )
            if dq.violations:
                raise DataQualityError("; ".join(dq.violations.values()))

            now_utc = datetime.utcnow()
            metadata_store.update_checkpoint(
                metadata_job_name,
                last_timestamp=last_timestamp,
                records_extracted=total_stats["sales"],
                status='IN_PROGRESS',
                date_range_start=start_date,
                date_range_end=end_date,
                chunk_number=chunk_number,
                chunk_row_count=stats["sales"],
                chunk_duration_seconds=chunk_duration,
                chunk_completed_at=now_utc,
            )

            metrics_emitter.emit_chunk_metrics(
                chunk_number=chunk_number,
                duration_seconds=chunk_duration,
                row_count=stats["sales"],
                api_calls_in_chunk=calls_in_current_chunk,
                last_timestamp=last_timestamp,
                retries=retries_in_chunk,
            )

            save_progress(
                job_id=progress_job_id,
                last_timestamp=last_timestamp,
                call_count=call_count,
                start_date=start_date,
                end_date=end_date,
                chunk_size=chunk_controller.current_chunk_size,
                total_stats=total_stats,
                status='IN_PROGRESS'
            )
        finally:
            chunk_count = chunk_number
            accumulated_sales = []
            calls_in_current_chunk = 0
            retries_in_chunk = 0
            chunk_controller.adjust_after_flush(chunk_duration)
            chunk_controller.reset_chunk_window()
            print(f"[{reason_label} {chunk_number}] [OK] Data & Progress saved! Memory cleared.")
            print(f"  Total so far: {total_stats['sales']:,} sales, {total_stats['items']:,} items")
            print(f"  Resume point: API call #{call_count}, timestamp={last_timestamp}")
        print()
    
    session = get_api_session()
    
    try:
        while True:
            if MAX_API_CALLS and call_count >= MAX_API_CALLS:
                print(f"  [SAFETY CAP] Reached {MAX_API_CALLS} calls")
                flush_chunk("SAFETY_CAP")
                break
            
            call_count += 1
            calls_in_current_chunk += 1
            
                url_path = f"/apps/v2/sync/sales?limit={BATCH_SIZE}&mode=ByDateTime"
                if last_timestamp:
                    url_path += f"&starttimestamp={last_timestamp}"
                url = f"https://{API_HOST}{url_path}"
            
            try:
                res, latency, retries_used = perform_api_call(session, url, metrics_emitter)
            except RuntimeError as api_err:
                print(f"  [FATAL] {api_err}")
                    break
                
            chunk_controller.record_latency(latency)
            total_retries += retries_used
            retries_in_chunk += retries_used
            
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
                
                def _parse_any_date(sale):
                    for key, fmts in [
                        ('businessDateTime', ["%Y-%m-%d %H:%M:%S"]),
                        ('businessDate', ["%Y-%m-%d"]),
                        ('systemDateTime', ["%Y-%m-%d %H:%M:%S"]),
                    ]:
                        val = sale.get(key)
                        if not val:
                            continue
                        for fmt in fmts:
                            try:
                                return datetime.strptime(val, fmt)
                            except Exception:
                                continue
                    return None
                
            batch_dates = [dt for sale in sales_batch if (dt := _parse_any_date(sale))]
            
                if call_count % 10 == 0 or batch_dates:
                    if batch_dates:
                        min_date = min(batch_dates)
                        max_date = max(batch_dates)
                        latest_date_overall = max_date
                        print(f"[Call {call_count}] {min_date.date()} to {max_date.date()} | {len(sales_batch)} records | Total: {len(accumulated_sales) + len(sales_batch):,}")
                    else:
                        print(f"[Call {call_count}] {len(sales_batch)} records (no date) | Total: {len(accumulated_sales) + len(sales_batch):,}")
                
                if enable_early_exit and batch_dates:
                    max_date = max(batch_dates)
                    if max_date > buffer_end:
                        in_range = [d for d in batch_dates if target_start <= d <= target_end]
                        if not in_range:
                            consecutive_out_of_range += 1
                            if consecutive_out_of_range >= 3:
                                print()
                                print(f"[SMART EXIT] 3 consecutive batches beyond target range")
                                print(f"  Latest date: {max_date.date()}")
                                print(f"  Target end: {end_date} (+ {buffer_days} buffer)")
                                accumulated_sales.extend(sales_batch)
                            flush_chunk("SMART_EXIT")
                                break
                        else:
                            consecutive_out_of_range = 0
                
                accumulated_sales.extend(sales_batch)
                last_timestamp = response.get('data', {}).get('lastTimestamp')
                if not last_timestamp:
                    print(f"  [COMPLETE] No more timestamps available")
                    break
                
            if calls_in_current_chunk >= chunk_controller.current_chunk_size:
                flush_chunk("CHECKPOINT")
        
        if accumulated_sales:
            flush_chunk("FINAL CHUNK")
        
        total_stats["api_calls"] = call_count
        
        metadata_store.update_checkpoint(
            metadata_job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='COMPLETED',
            date_range_start=start_date,
            date_range_end=end_date,
            chunk_number=None,
            chunk_row_count=None,
            chunk_duration_seconds=None,
            chunk_completed_at=datetime.utcnow(),
        )
        
        save_progress(
            job_id=progress_job_id,
            last_timestamp=last_timestamp,
            call_count=call_count,
            start_date=start_date,
            end_date=end_date,
            chunk_size=chunk_controller.current_chunk_size,
            total_stats=total_stats,
            status='COMPLETED'
        )
        
        print()
        print("="*80)
        print(" "*25 + "CHUNKED EXTRACTION COMPLETE")
        print("="*80)
        print(f"  Total API Calls: {call_count}")
        print(f"  Total Chunks Saved: {chunk_count}")
        print(f"  Sales Loaded: {total_stats['sales']:,}")
        print(f"  Items Loaded: {total_stats['items']:,}")
        print(f"  Payments Loaded: {total_stats['payments']:,}")
        print(f"  Total Retries: {total_retries}")
        if latest_date_overall:
            print(f"  Latest Date Reached: {latest_date_overall.date()}")
        print()
        print("[SUCCESS] All data safely loaded to staging tables!")
        print("="*80)
        print()
        
        return total_stats
        
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] Extraction stopped by user")
        print(f"  Progress saved: {total_stats['sales']:,} sales in {chunk_count} chunks")
        print(f"  Resume by running again - it will continue from last checkpoint")
        
        # Save interrupted state
        if last_timestamp:
            save_progress(
                job_id=progress_job_id,
                last_timestamp=last_timestamp,
                call_count=call_count,
                start_date=start_date,
                end_date=end_date,
                chunk_size=chunk_controller.current_chunk_size,
                total_stats=total_stats,
                status='INTERRUPTED'
            )
        metadata_store.update_checkpoint(
            metadata_job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='INTERRUPTED',
            date_range_start=start_date,
            date_range_end=end_date,
            chunk_number=None,
            chunk_row_count=None,
            chunk_duration_seconds=None,
            chunk_completed_at=datetime.utcnow(),
            )
        
        return total_stats
    except DataQualityError as dq_err:
        print()
        print(f"[DATA QUALITY FAILURE] {dq_err}")
        if last_timestamp:
            save_progress(
                job_id=progress_job_id,
                last_timestamp=last_timestamp,
                call_count=call_count,
                start_date=start_date,
                end_date=end_date,
                chunk_size=chunk_controller.current_chunk_size,
                total_stats=total_stats,
                status='ERROR',
                error_msg=str(dq_err)
            )
        metadata_store.update_checkpoint(
            metadata_job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='ERROR',
            date_range_start=start_date,
            date_range_end=end_date,
            error_message=str(dq_err),
            chunk_number=None,
            chunk_row_count=None,
            chunk_duration_seconds=None,
            chunk_completed_at=datetime.utcnow(),
        )
        raise
    except Exception as e:
        print()
        print(f"[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        
        # Save error state
        if last_timestamp:
            save_progress(
                job_id=progress_job_id,
                last_timestamp=last_timestamp,
                call_count=call_count,
                start_date=start_date,
                end_date=end_date,
                chunk_size=chunk_controller.current_chunk_size,
                total_stats=total_stats,
                status='ERROR',
                error_msg=str(e)
            )
        metadata_store.update_checkpoint(
            metadata_job_name,
            last_timestamp=last_timestamp,
            records_extracted=total_stats["sales"],
            status='ERROR',
            date_range_start=start_date,
            date_range_end=end_date,
            error_message=str(e),
            chunk_number=None,
            chunk_row_count=None,
            chunk_duration_seconds=None,
            chunk_completed_at=datetime.utcnow(),
            )
        
        return total_stats

