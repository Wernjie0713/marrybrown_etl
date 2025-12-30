"""
Replication utilities for reference tables and general exports.

Adds a direct streaming mode for --full-table runs to avoid Parquet I/O while
keeping existing Parquet-based flows and helpers for compatibility.
"""

import argparse
import json
import sys
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

# Add parent directory to path to import config
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

import config

REPLICA_SCHEMA_PATH = PROJECT_ROOT / "docs" / "replica_schema.json"
FULL_SCHEMA_PATH = PROJECT_ROOT / "docs" / "xilnex_full_schema.json"

# SQL Server DATETIME range limits
DATETIME_MIN = datetime(1753, 1, 1, 0, 0, 0)
DATETIME_MAX = datetime(9999, 12, 31, 23, 59, 59)


def round_to_datetime_precision(dt: datetime) -> datetime:
    """
    Round datetime to SQL Server DATETIME precision.
    
    DATETIME has precision of 1/300 second (approximately 3.33ms).
    This function rounds microseconds to the nearest 1/300 second increment.
    
    Example:
    - 123456 microseconds → rounds to nearest 3333 microsecond increment
    - Result: datetime with microseconds rounded to valid DATETIME precision
    """
    if dt.microsecond == 0:
        return dt
    
    # DATETIME precision: 1/300 second = 3333.33... microseconds
    # Round to nearest 3333 microsecond increment
    increment = 1000000 / 300  # Approximately 3333.33 microseconds
    rounded_microseconds = round(dt.microsecond / increment) * increment
    
    # Ensure we don't exceed 999999 microseconds
    if rounded_microseconds >= 1000000:
        # Round up to next second, reset microseconds to 0
        return dt.replace(microsecond=0) + timedelta(seconds=1)
    
    # Round to integer microseconds
    rounded_microseconds = int(rounded_microseconds)
    
    return dt.replace(microsecond=rounded_microseconds)

# Tables that support date filtering and the column to use
DATE_FILTER_COLUMNS = {
    "APP_4_SALES": "DATETIME__SALES_DATE",
    "APP_4_SALESITEM": "DATETIME__SALES_DATE",
    "APP_4_RECIPESUMMARY": "DATETIME__TRANSACTION_DATETIME",
    "APP_4_SALESQUANTITIES": "SALES_DATE",
    "APP_4_ORDER": "DATETIME__ORDER_DATE",
    "APP_4_ORDERITEM": "ORDER_DATE",
    "APP_4_PAYMENT": "DATETIME__DATE",
    "APP_4_VOIDSALESITEM": "DATETIME__VOID_DATETIME",
    "APP_4_SALESCREDITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESCREDITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_EPAYMENTLOG": "TRANSACTIONDATETIME",
    "APP_4_VOUCHER": "DATETIME__VOUCHER_DATE",
}

# Compression mapping
COMPRESSION_MAP = {
    "snappy": "snappy",
    "gzip": "gzip",
    "zstd": "zstd",
    "none": None,
    "uncompressed": None,
}


class ConnectionManager:
    """Context manager for database connections with pooling."""
    
    def __init__(self, source_conn=None, target_conn=None):
        self.source_conn = source_conn
        self.target_conn = target_conn
        self._source_created = source_conn is None
        self._target_created = target_conn is None
    
    def __enter__(self):
        if self._source_created and self.source_conn is None:
            self.source_conn = get_source_connection()
        if self._target_created and self.target_conn is None:
            self.target_conn = get_target_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._source_created and self.source_conn:
            self.source_conn.close()
        if self._target_created and self.target_conn:
            self.target_conn.close()


def load_schema() -> Dict[str, dict]:
    """Load actual table schemas from xilnex_full_schema.json."""
    # Get table names from replica_schema.json
    replica_data = json.loads(REPLICA_SCHEMA_PATH.read_text(encoding="utf-8"))
    table_names = [t["name"] for t in replica_data["tables"]]
    
    # Load full Xilnex schema
    full_schema = json.loads(FULL_SCHEMA_PATH.read_text(encoding="utf-8"))
    
    # Extract schemas for our tables
    result = {}
    for table_name in table_names:
        full_table_key = f"COM_5013.{table_name}"
        if full_table_key not in full_schema:
            # Try to find it
            for key in full_schema.keys():
                if key.endswith(f".{table_name}"):
                    full_table_key = key
                    break
            else:
                print(f"[WARN] Table {table_name} not found in xilnex_full_schema.json", file=sys.stderr)
                continue
        
        schema_entry = full_schema[full_table_key]
        # Convert to format expected by rest of code
        result[table_name] = {
            "name": table_name,
            "schema": schema_entry.get("schema", "COM_5013"),
            "columns": sorted(schema_entry["columns"], key=lambda x: x.get("ordinal_position", 999))
        }
    
    return result


def get_source_connection():
    conn_str = config.build_connection_string(config.AZURE_SQL_CONFIG)
    return pyodbc.connect(conn_str)


def get_target_connection():
    # Trust server certificate for local connections
    conn_str = config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    return pyodbc.connect(conn_str)


def build_select_statement(
    table_name: str,
    schema_entry: dict,
    start_date: Optional[str],
    end_date: Optional[str],
    full_table: bool,
) -> Tuple[str, List]:
    columns = [col["name"] for col in schema_entry["columns"]]
    select_clause = ", ".join(columns)
    source_table = f"{schema_entry.get('schema', 'COM_5013')}.{table_name}"

    where_clauses = []
    params: List = []
    date_column = DATE_FILTER_COLUMNS.get(table_name)

    if not full_table and date_column and start_date:
        where_clauses.append(f"{date_column} >= ?")
        params.append(start_date)
        if end_date:
            where_clauses.append(f"{date_column} < ?")
            params.append(end_date)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    query = f"SELECT {select_clause} FROM {source_table}{where_sql}"
    return query, params


def optimize_dataframe_dtypes(df: pd.DataFrame, first_chunk: bool = False, allow_category: bool = False) -> pd.DataFrame:
    """Optimize DataFrame dtypes to reduce memory usage.
    
    Args:
        df: DataFrame to optimize
        first_chunk: Whether this is the first chunk (used for category conversion)
        allow_category: Whether to allow category dtype conversion (disabled for Parquet writing)
    """
    # 1:1 replication requirement: do not alter dtypes (no downcasting/category conversion)
    # This function remains for interface compatibility but acts as a no-op.
    return df


def prepare_data_for_sql(df: pd.DataFrame, schema_entry: dict) -> pd.DataFrame:
    """Prepare DataFrame for SQL insertion with robust type conversion."""
    columns = [col["name"] for col in schema_entry["columns"]]
    df = df[[col for col in columns if col in df.columns]].copy()
    
    column_type_map = {col["name"]: col.get("type", "").lower() for col in schema_entry["columns"]}
    placeholder_count = {}
    
    for col_name in df.columns:
        if col_name not in column_type_map:
            continue
        
        col_type = column_type_map[col_name]
        if col_type in ("datetime", "date"):
            def convert_datetime_value(val):
                if val is None:
                    return None
                if isinstance(val, (float, int)) and pd.isna(val):
                    return None
                
                dt = None
                if isinstance(val, (datetime, pd.Timestamp)):
                    if pd.isna(val):
                        return None
                    dt = val.to_pydatetime() if isinstance(val, pd.Timestamp) else val
                elif isinstance(val, date):
                    dt = datetime.combine(val, datetime.min.time())
                elif isinstance(val, str):
                    s_val = val.strip()
                    if not s_val:
                        return None
                    try:
                        if len(s_val) == 19 and s_val[10] == " ":
                            dt = datetime.strptime(s_val, "%Y-%m-%d %H:%M:%S")
                        elif len(s_val) == 10 and "-" in s_val:
                            dt = datetime.strptime(s_val, "%Y-%m-%d")
                        else:
                            parsed = pd.to_datetime(s_val)
                            if pd.isna(parsed):
                                return None
                            dt = parsed.to_pydatetime()
                    except (ValueError, TypeError):
                        return None
                
                if dt is not None:
                    if dt < DATETIME_MIN or dt > DATETIME_MAX:
                        placeholder_count[col_name] = placeholder_count.get(col_name, 0) + 1
                        return None
                    if col_type == "date":
                        return dt.date()
                    return round_to_datetime_precision(dt)
                
                return None
            
            df[col_name] = df[col_name].apply(convert_datetime_value)
            
            if col_name in placeholder_count:
                print(
                    f"[WARN] {col_name}: Converted {placeholder_count[col_name]} placeholder date(s) "
                    f"(out of DATETIME range {DATETIME_MIN.date()} to {DATETIME_MAX.date()}) to NULL"
                )
    
    df = df.astype(object)
    df = df.where(pd.notnull(df), None)
    
    def sanitize_and_convert(val):
        if val is None:
            return None
        if hasattr(val, "item"):
            val = val.item()
        if isinstance(val, float) and (val != val):
            return None
        return val
    
    for col_name in df.columns:
        df[col_name] = df[col_name].apply(sanitize_and_convert)

    # Final sweep: ensure no pandas NA/NaT survived the sanitize pass
    df = df.where(pd.notnull(df), None)

    return df


def coerce_python_value(val):
    """Convert numpy/pandas scalar types to native Python types, mapping NaN/NaT to None."""
    if val is None or val is pd.NaT:
        return None
    if hasattr(val, "item"):
        try:
            val = val.item()
        except Exception:
            pass
    if isinstance(val, float) and pd.isna(val):
        return None
    return val


def build_row_tuple(row_iterable):
    """Build a tuple ready for pyodbc executemany, ensuring only native Python types."""
    return tuple(coerce_python_value(v) for v in row_iterable)


def estimate_optimal_chunk_size(column_count: int, available_memory_mb: int = None) -> int:
    """Estimate optimal chunk size based on column count and available memory."""
    if available_memory_mb is None:
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
    
    # Rough estimate: each row with many columns uses ~1-2KB
    # Use 10% of available memory for chunk
    estimated_rows_per_mb = 500  # Conservative estimate
    target_memory_mb = available_memory_mb * 0.1
    
    optimal_size = int(target_memory_mb * estimated_rows_per_mb)
    
    # Adjust for column count (more columns = smaller chunks)
    if column_count > 100:
        optimal_size = int(optimal_size * (100 / column_count))
    
    # Clamp between 10K and 500K
    return max(10000, min(500000, optimal_size))


def write_parquet_incremental(
    chunks: Iterator[pd.DataFrame],
    output_path: Path,
    compression: str = "snappy",
) -> int:
    """Write Parquet file incrementally using PyArrow ParquetWriter.
    
    Handles schema inference properly by ensuring all object columns are nullable strings,
    even if first chunk has all NULLs.
    """
    total_rows = 0
    parquet_writer = None
    schema = None
    chunk_idx = 0
    
    try:
        for chunk in chunks:
            if chunk.empty:
                continue
            
            # Optimize chunk (disable category conversion to maintain Parquet schema consistency)
            chunk = optimize_dataframe_dtypes(chunk, first_chunk=(chunk_idx == 0), allow_category=False)
            
            # Fix NULL type inference: Ensure all object columns are treated as nullable strings
            # This prevents schema mismatch when first chunk has all NULLs but later chunks have values
            # PyArrow will infer null type if all values are NULL, but we want nullable string
            for col in chunk.select_dtypes(include=['object']).columns:
                # Ensure column is object dtype (nullable) - this preserves NULLs
                # PyArrow will convert object dtype with NULLs to nullable string type
                if chunk[col].isna().all():
                    # If all NULLs, ensure it's object dtype so PyArrow infers nullable string
                    chunk[col] = chunk[col].astype('object')
            
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            
            # Fix null type inference: If any columns are null type (all NULLs in first chunk),
            # cast them to nullable string type to prevent schema mismatches
            if schema is None:
                # First chunk - fix null types before setting schema
                fields = []
                for field in table.schema:
                    if pa.types.is_null(field.type):
                        # Convert null type to nullable string
                        fields.append(pa.field(field.name, pa.string(), nullable=True))
                    else:
                        fields.append(field)
                
                if fields != list(table.schema):
                    # Recreate table with fixed schema
                    schema_dict = {field.name: field.type for field in fields}
                    table = table.cast(pa.schema(fields), safe=False)
                
                schema = table.schema
                parquet_writer = pq.ParquetWriter(
                    output_path,
                    schema,
                    compression=COMPRESSION_MAP.get(compression, "snappy"),
                    use_dictionary=True,
                )
            else:
                # Subsequent chunks - unify schemas if they differ
                if schema != table.schema:
                    try:
                        # Try to unify schemas (PyArrow will promote types as needed)
                        unified_schema = pa.unify_schemas([schema, table.schema])
                        if unified_schema != schema:
                            # Schema changed, need to recreate writer
                            parquet_writer.close()
                            schema = unified_schema
                            parquet_writer = pq.ParquetWriter(
                                output_path,
                                schema,
                                compression=COMPRESSION_MAP.get(compression, "snappy"),
                                use_dictionary=True,
                            )
                        # Cast table to unified schema
                        table = table.cast(schema, safe=False)
                    except Exception:
                        # If unification fails, cast table to match existing schema
                        table = table.cast(schema, safe=False)
            
            # Write chunk
            parquet_writer.write_table(table)
            total_rows += len(chunk)
            chunk_idx += 1
        
        if parquet_writer:
            parquet_writer.close()
    except Exception as e:
        if parquet_writer:
            parquet_writer.close()
        # Clean up partial file on error
        if output_path.exists():
            output_path.unlink()
        raise e
    
    return total_rows


def validate_columns(table_name: str, schema_entry: dict, actual_columns) -> None:
    expected = [col["name"] for col in schema_entry["columns"]]
    missing = [col for col in expected if col not in actual_columns]
    extra = [col for col in actual_columns if col not in expected]
    if missing:
        print(
            f"[WARN] {table_name}: missing columns in source export: {', '.join(missing)}",
            file=sys.stderr,
        )
    if extra:
        print(
            f"[WARN] {table_name}: unexpected columns present: {', '.join(extra)}",
            file=sys.stderr,
        )


def analyze_data_requirements(parquet_path: Path, schema_entry: dict) -> dict:
    """Analyze Parquet data to determine actual requirements for schema adjustment.
    
    Returns dict with column requirements:
    - string columns: max_length needed
    - decimal columns: is_integer flag (to use DECIMAL(38,0) vs DECIMAL(38,20))
    - integer columns: type needed (INT/BIGINT)
    
    Simplified approach: Use maximum DECIMAL sizes (38,0) for integers, (38,20) for decimals.
    """
    requirements = {}
    
    if not parquet_path.exists() or parquet_path.stat().st_size == 0:
        return requirements
    
    # Read Parquet file - analyze ALL batches, not just a sample
    parquet_file = pq.ParquetFile(parquet_path)
    
    # Process all batches to ensure we catch all values
    for batch in parquet_file.iter_batches(batch_size=50000):
        df = batch.to_pandas()
        if df.empty:
            continue
        
        for col_info in schema_entry["columns"]:
            col_name = col_info["name"]
            if col_name not in df.columns:
                continue
            
            col_type = col_info["type"].upper()
            
            if col_name not in requirements:
                requirements[col_name] = {
                    "type": col_type,
                    "source_precision": col_info.get("numeric_precision"),
                    "source_scale": col_info.get("numeric_scale"),
                    "source_char_len": col_info.get("char_len"),
                }
            
            # Analyze string columns
            if col_type in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR"):
                mask = df[col_name].notna()
                if mask.any():
                    str_lengths = df.loc[mask, col_name].astype(str).str.len()
                    max_len = str_lengths.max()
                    current_max = requirements[col_name].get("max_length", 0)
                    requirements[col_name]["max_length"] = max(current_max, max_len)
            
            # Analyze DECIMAL/NUMERIC columns
            elif col_type in ("DECIMAL", "NUMERIC"):
                mask = df[col_name].notna()
                if mask.any():
                    numeric_values = pd.to_numeric(df.loc[mask, col_name], errors='coerce')
                    valid_values = numeric_values.dropna()
                    
                    if not valid_values.empty:
                        # Simple approach: Check if all values are integers
                        # If integers -> use DECIMAL(38,0), if decimals -> use DECIMAL(38,20)
                        all_integers = True
                        
                        # Check ALL values to see if they're integers
                        for val in valid_values:
                            if not pd.isna(val):
                                val_str = str(val)
                                # Check if string representation has non-zero fractional part
                                if '.' in val_str:
                                    # Check if fractional part is all zeros
                                    frac_part = val_str.split('.')[1]
                                    # Remove trailing zeros and check if anything remains
                                    if frac_part.rstrip('0'):
                                        all_integers = False
                                        break
                        
                        # Store whether this column contains only integers
                        if "is_integer" not in requirements[col_name]:
                            requirements[col_name]["is_integer"] = all_integers
                        else:
                            requirements[col_name]["is_integer"] = requirements[col_name]["is_integer"] and all_integers
            
            # Analyze integer columns
            elif col_type in ("INT", "SMALLINT", "TINYINT"):
                mask = df[col_name].notna()
                if mask.any():
                    numeric_values = pd.to_numeric(df.loc[mask, col_name], errors='coerce')
                    valid_values = numeric_values.dropna()
                    
                    if not valid_values.empty:
                        max_val = valid_values.max()
                        min_val = valid_values.min()
                        
                        # Determine required type
                        if max_val > 2147483647 or min_val < -2147483648:
                            requirements[col_name]["required_type"] = "BIGINT"
                        elif max_val > 32767 or min_val < -32768:
                            requirements[col_name]["required_type"] = "INT"
                        elif max_val > 255 or min_val < 0:
                            requirements[col_name]["required_type"] = "SMALLINT"
                        else:
                            requirements[col_name]["required_type"] = "TINYINT"
    
    return requirements


def get_target_table_schema(
    table_name: str,
    conn_manager: Optional[ConnectionManager] = None,
    cursor: Optional[pyodbc.Cursor] = None,
) -> dict:
    """Get the actual current schema of the target table from SQL Server."""
    target_table = f"dbo.com_5013_{table_name}"
    
    # Use provided cursor/connection or create new
    if cursor is not None:
        conn = None
        close_conn = False
    elif conn_manager and conn_manager.target_conn:
        conn = conn_manager.target_conn
        close_conn = False
        cursor = conn.cursor()
    else:
        conn = get_target_connection()
        close_conn = True
        cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' 
            AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, target_table.replace('dbo.', ''))
        
        schema = {}
        for row in cursor.fetchall():
            col_name, data_type, char_len, num_precision, num_scale = row
            schema[col_name] = {
                "type": data_type,
                "char_len": char_len,
                "numeric_precision": num_precision,
                "numeric_scale": num_scale,
            }
        
        return schema
    finally:
        if close_conn and conn:
            conn.close()




def load_via_bulk_insert(
    table_name: str,
    schema_entry: dict,
    parquet_path: Path,
    start_date: Optional[str],
    end_date: Optional[str],
    conn_manager: Optional[ConnectionManager] = None,
) -> int:
    """Load data using SQL Server BULK INSERT from Parquet file."""
    target_table = f"dbo.com_5013_{table_name}"
    
    # Use provided connection or create new
    if conn_manager and conn_manager.target_conn:
        conn = conn_manager.target_conn
        close_conn = False
    else:
        conn = get_target_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        
        # Delete existing range if date-filtered
        delete_existing_range(
            cursor, target_table, DATE_FILTER_COLUMNS.get(table_name), start_date, end_date
        )
        
        # Convert Windows path to format SQL Server can access
        # For local SQL Server, use the file path directly
        parquet_file_path = str(parquet_path).replace('\\', '/')
        
        # Try OPENROWSET first (SQL Server 2017+)
        try:
            # Get column list
            columns = [col["name"] for col in schema_entry["columns"]]
            column_list = ", ".join(columns)
            
            # Use OPENROWSET to read Parquet
            sql = f"""
            INSERT INTO {target_table} ({column_list})
            SELECT {column_list}
            FROM OPENROWSET(
                BULK '{parquet_file_path}',
                FORMAT = 'PARQUET'
            ) AS [parquet_file]
            """
            
            cursor.execute(sql)
            rows_loaded = cursor.rowcount
            conn.commit()
            print(f"[LOAD] {table_name}: loaded {rows_loaded:,} rows via BULK INSERT")
            return rows_loaded
        except Exception as e:
            # Fallback to regular batch loading
            print(f"[WARN] BULK INSERT failed, falling back to batch loading: {e}", file=sys.stderr)
            # Read Parquet and load in batches
            df = pd.read_parquet(parquet_path, engine="pyarrow")
            return load_in_batches(
                table_name,
                schema_entry,
                df,
                start_date,
                end_date,
                batch_size=100000,
                commit_interval=100000,
                conn_manager=conn_manager,
            )
    finally:
        if close_conn:
            conn.close()


def load_from_parquet_streaming(
    table_name: str,
    schema_entry: dict,
    parquet_path: Path,
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int = 100000,
    commit_interval: int = 100000,
    conn_manager: Optional[ConnectionManager] = None,
) -> int:
    """Load from Parquet file in streaming fashion (no full DataFrame in memory)."""
    # Check if file exists and has data
    if not parquet_path.exists():
        print(f"[WARN] {table_name}: Parquet file not found: {parquet_path}")
        return 0
    
    # Check file size (empty Parquet files can still exist)
    if parquet_path.stat().st_size == 0:
        print(f"[WARN] {table_name}: Parquet file is empty: {parquet_path}")
        return 0
    
    target_table = f"dbo.com_5013_{table_name}"
    columns = [col["name"] for col in schema_entry["columns"]]
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    
    # Use provided connection or create new
    if conn_manager and conn_manager.target_conn:
        conn = conn_manager.target_conn
        close_conn = False
    else:
        conn = get_target_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        
        # Delete existing range if date-filtered
        delete_existing_range(
            cursor, target_table, DATE_FILTER_COLUMNS.get(table_name), start_date, end_date
        )
        
        cursor.fast_executemany = True
        total_loaded = 0
        rows_since_commit = 0
        
        # Read Parquet in chunks using PyArrow's iter_batches (pd.read_parquet doesn't support chunksize)
        parquet_file = pq.ParquetFile(parquet_path)
        
        for batch_idx, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
            # Convert PyArrow batch to pandas DataFrame
            batch_df = batch.to_pandas()
            
            if batch_df.empty:
                continue
            
            # Prepare data for SQL
            batch_df = prepare_data_for_sql(batch_df, schema_entry)
            
            # Pre-insert validation: convert any remaining NaN/NaT to NULL (None)
            # Log conversions for visibility
            for col_name in batch_df.columns:
                # Check each value for NaN/NaT using same logic as conversion function
                for idx, val in enumerate(batch_df[col_name]):
                    if val is None:
                        continue
                    
                    # Check for NaN/NaT FIRST using pandas (same as conversion function)
                    try:
                        if pd.isna(val):
                            batch_df.iat[idx, batch_df.columns.get_loc(col_name)] = None
                            continue
                    except (TypeError, ValueError):
                        pass
                    
                    # Explicit check for float NaN (fallback)
                    if isinstance(val, (float, np.floating)):
                        try:
                            if np.isnan(val):
                                batch_df.iat[idx, batch_df.columns.get_loc(col_name)] = None
                                continue
                        except (TypeError, ValueError):
                            pass
                        if isinstance(val, float) and (val != val):
                            batch_df.iat[idx, batch_df.columns.get_loc(col_name)] = None
                            continue
                    
                    # Explicit check for NaT
                    if val is pd.NaT:
                        batch_df.iat[idx, batch_df.columns.get_loc(col_name)] = None
                        continue
            
            # Validate numeric values before insert (catch issues early)
            try:
                batch_data = [
                    build_row_tuple(row)
                    for row in batch_df[columns].itertuples(index=False, name=None)
                ]
            except Exception as e:
                print(f"[ERROR] {table_name}: Failed to prepare batch {batch_idx}: {e}", file=sys.stderr)
                raise
            
            try:
                cursor.executemany(
                    f"INSERT INTO {target_table} ({column_list}) VALUES ({placeholders})",
                    batch_data,
                )
            except Exception as e:
                # If we get a numeric error, try to identify the problematic column/value
                error_msg = str(e)
                if "Numeric value out of range" in error_msg or "Fractional truncation" in error_msg:
                    print(f"[ERROR] {table_name}: Numeric error in batch {batch_idx}. Analyzing problematic values...", file=sys.stderr)
                    # Try to identify which column is causing the issue
                    for col_info in schema_entry["columns"]:
                        col_name = col_info["name"]
                        if col_name not in batch_df.columns:
                            continue
                        col_type = col_info["type"].upper()
                        if col_type in ("DECIMAL", "NUMERIC"):
                            numeric_values = pd.to_numeric(batch_df[col_name], errors='coerce')
                            valid_values = numeric_values.dropna()
                            if not valid_values.empty:
                                max_val = valid_values.max()
                                min_val = valid_values.min()
                                print(f"[DEBUG] {col_name}: min={min_val}, max={max_val}, dtype={batch_df[col_name].dtype}", file=sys.stderr)
                        elif col_type in ("INT", "SMALLINT", "TINYINT", "BIGINT"):
                            numeric_values = pd.to_numeric(batch_df[col_name], errors='coerce')
                            valid_values = numeric_values.dropna()
                            if not valid_values.empty:
                                max_val = valid_values.max()
                                min_val = valid_values.min()
                                print(f"[DEBUG] {col_name} (INT): min={min_val}, max={max_val}, dtype={batch_df[col_name].dtype}", file=sys.stderr)
                raise
            
            total_loaded += len(batch_df)
            rows_since_commit += len(batch_df)
            
            # Commit at intervals
            if rows_since_commit >= commit_interval:
                conn.commit()
                rows_since_commit = 0
                print(f"  [LOAD] {table_name}: committed {total_loaded:,} rows", end="\r", flush=True)
        
        # Final commit
        conn.commit()
        print(f"\n[LOAD] {table_name}: loaded {total_loaded:,} rows into {target_table}")
        
        return total_loaded
    except Exception as e:
        print(f"[ERROR] {table_name}: Failed to load from Parquet: {e}", file=sys.stderr)
        raise
    finally:
        if close_conn:
            conn.close()


def load_in_batches(
    table_name: str,
    schema_entry: dict,
    df: pd.DataFrame,
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int = 100000,
    commit_interval: int = 100000,
    conn_manager: Optional[ConnectionManager] = None,
) -> int:
    """Load DataFrame into target database in batches."""
    if df.empty:
        print(f"[LOAD] {table_name}: nothing to load")
        return 0

    target_table = f"dbo.com_5013_{table_name}"
    columns = [col["name"] for col in schema_entry["columns"]]
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    
    # Prepare data
    df = prepare_data_for_sql(df, schema_entry)
    
    # Pre-insert validation: convert any remaining NaN/NaT to NULL (None)
    for col_name in df.columns:
        for idx, val in enumerate(df[col_name]):
            if val is None:
                continue
            try:
                if pd.isna(val):
                    df.iat[idx, df.columns.get_loc(col_name)] = None
                    continue
            except (TypeError, ValueError):
                pass
            if isinstance(val, (float, np.floating)):
                try:
                    if np.isnan(val):
                        df.iat[idx, df.columns.get_loc(col_name)] = None
                        continue
                except (TypeError, ValueError):
                    pass
                if isinstance(val, float) and (val != val):
                    df.iat[idx, df.columns.get_loc(col_name)] = None
                    continue
            if val is pd.NaT:
                df.iat[idx, df.columns.get_loc(col_name)] = None
                continue
    
    # Use provided connection or create new
    if conn_manager and conn_manager.target_conn:
        conn = conn_manager.target_conn
        close_conn = False
    else:
        conn = get_target_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        
        # Delete existing range if date-filtered
        delete_existing_range(
            cursor, target_table, DATE_FILTER_COLUMNS.get(table_name), start_date, end_date
        )
        
        cursor.fast_executemany = True
        total_loaded = 0
        rows_since_commit = 0
        
        # Process in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_data = [
                build_row_tuple(row)
                for row in batch[columns].itertuples(index=False, name=None)
            ]
            
            cursor.executemany(
                f"INSERT INTO {target_table} ({column_list}) VALUES ({placeholders})",
                batch_data,
            )
            
            total_loaded += len(batch)
            rows_since_commit += len(batch)
            
            # Commit at intervals
            if rows_since_commit >= commit_interval:
                conn.commit()
                rows_since_commit = 0
                print(f"  [LOAD] {table_name}: committed {total_loaded:,} rows", end="\r", flush=True)
        
        # Final commit
        conn.commit()
        print(f"\n[LOAD] {table_name}: loaded {total_loaded:,} rows into {target_table}")
        
        return total_loaded
    finally:
        if close_conn:
            conn.close()


def delete_existing_range(
    cursor: pyodbc.Cursor,
    target_table: str,
    date_column: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
):
    if not date_column or not start_date:
        return
    if end_date:
        cursor.execute(
            f"DELETE FROM {target_table} WHERE {date_column} >= ? AND {date_column} < ?",
            start_date,
            end_date,
        )
    else:
        cursor.execute(
            f"DELETE FROM {target_table} WHERE {date_column} = ?",
            start_date,
        )


def stream_full_table_direct(
    table_name: str,
    schema_entry: dict,
    start_date: Optional[str],
    end_date: Optional[str],
    args: argparse.Namespace,
    conn_manager: Optional[ConnectionManager] = None,
) -> int:
    """Stream a full table directly from source to target without Parquet."""
    query, params = build_select_statement(
        table_name,
        schema_entry,
        start_date,
        end_date,
        full_table=True,
    )

    columns = [col["name"] for col in schema_entry["columns"]]
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    insert_sql = f"INSERT INTO dbo.com_5013_{table_name} ({column_list}) VALUES ({placeholders})"

    print(f"\n[STREAM] {table_name}: streaming full table directly to target")

    # Connections
    if conn_manager and conn_manager.source_conn:
        source_conn = conn_manager.source_conn
        close_source = False
    else:
        source_conn = get_source_connection()
        close_source = True

    if conn_manager and conn_manager.target_conn:
        target_conn = conn_manager.target_conn
        close_target = False
    else:
        target_conn = get_target_connection()
        close_target = True

    try:
        cursor = target_conn.cursor()
        cursor.fast_executemany = True

        total_loaded = 0
        rows_since_commit = 0
        first_chunk = True

        chunk_iter = pd.read_sql_query(
            query,
            source_conn,
            params=params if params else None,
            chunksize=args.chunk_size,
        )

        for chunk_idx, chunk in enumerate(chunk_iter):
            if chunk.empty:
                continue

            if first_chunk:
                validate_columns(table_name, schema_entry, chunk.columns)
                first_chunk = False

            chunk = prepare_data_for_sql(chunk, schema_entry)
            batch_data = [
                build_row_tuple(row)
                for row in chunk[columns].itertuples(index=False, name=None)
            ]

            try:
                cursor.executemany(insert_sql, batch_data)
            except Exception as exc:
                print(f"[ERROR] {table_name}: failed during direct stream chunk {chunk_idx}: {exc}", file=sys.stderr)
                raise

            total_loaded += len(batch_data)
            rows_since_commit += len(batch_data)

            if rows_since_commit >= args.commit_interval:
                target_conn.commit()
                rows_since_commit = 0
                print(f"  [STREAM] {table_name}: committed {total_loaded:,} rows", end="\r", flush=True)

        target_conn.commit()
        print(f"\n[STREAM] {table_name}: streamed {total_loaded:,} rows directly to target")
        return total_loaded
    finally:
        if close_source:
            source_conn.close()
        if close_target:
            target_conn.close()


def stream_export_and_load(
    table_name: str,
    schema_entry: dict,
    start_date: Optional[str],
    end_date: Optional[str],
    args: argparse.Namespace,
    conn_manager: Optional[ConnectionManager] = None,
) -> Tuple[Path, int, int]:
    """
    Stream export and load: process chunks one at a time.
    Returns: (parquet_path, total_rows, rows_loaded)
    """
    query, params = build_select_statement(
        table_name,
        schema_entry,
        start_date,
        end_date,
        args.full_table,
    )
    
    print(f"\n[EXPORT] {table_name}: running query")
    
    # Use provided connection or create new
    if conn_manager and conn_manager.source_conn:
        source_conn = conn_manager.source_conn
    else:
        source_conn = get_source_connection()
    
    # Prepare output path
    output_dir = Path(args.output_dir) / table_name.lower()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_suffix = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    file_name = f"{table_name.lower()}_{run_suffix}.parquet"
    output_path = output_dir / file_name
    
    # Stream chunks and write incrementally
    total_rows = 0
    first_chunk = True
    chunks_to_load = []
    
    try:
        chunk_iter = pd.read_sql_query(
            query, source_conn, params=params if params else None, chunksize=args.chunk_size
        )
        
        def chunk_generator():
            nonlocal total_rows, first_chunk
            for chunk in chunk_iter:
                if chunk.empty:
                    continue
                
                total_rows += len(chunk)
                print(f"  fetched {total_rows:,} rows", end="\r", flush=True)
                
                # Validate columns on first chunk
                if first_chunk:
                    validate_columns(table_name, schema_entry, chunk.columns)
                    first_chunk = False
                
                yield chunk
        
        # Write Parquet incrementally
        parquet_rows = write_parquet_incremental(
            chunk_generator(),
            output_path,
            compression=args.compression,
        )
        
        print(f"\n[EXPORT] {table_name}: wrote {parquet_rows:,} rows to {output_path}")
        
        # Load to SQL if not skipped
        rows_loaded = 0
        if not args.skip_load:
            if args.use_bulk_insert:
                # Use BULK INSERT from Parquet (faster but requires file accessible to SQL Server)
                rows_loaded = load_via_bulk_insert(
                    table_name,
                    schema_entry,
                    output_path,
                    start_date,
                    end_date,
                    conn_manager=conn_manager,
                )
            else:
                # Read Parquet file for loading (streaming, no full DataFrame)
                print(f"[LOAD] {table_name}: loading into target database")
                rows_loaded = load_from_parquet_streaming(
                    table_name,
                    schema_entry,
                    output_path,
                    start_date,
                    end_date,
                    batch_size=args.batch_size,
                    commit_interval=args.commit_interval,
                    conn_manager=conn_manager,
                )
        
        return output_path, parquet_rows, rows_loaded
        
    finally:
        if not conn_manager or not conn_manager.source_conn:
            source_conn.close()


def run_for_table(
    table_name: str,
    schema_entry: dict,
    args: argparse.Namespace,
    start_date: Optional[str],
    end_date: Optional[str],
    conn_manager: Optional[ConnectionManager] = None,
):
    """Process a single table using streaming pipeline."""
    output_dir = Path(args.output_dir) / table_name.lower()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine mode for full-table loads
    full_table_mode = args.full_table_mode
    if args.skip_load or args.use_bulk_insert:
        # Parquet is required if caller wants to skip load or use BULK INSERT
        full_table_mode = "parquet"
        if args.full_table_mode == "stream" and args.skip_load:
            print(f"[INFO] {table_name}: --skip-load forces full-table mode to parquet")
        if args.full_table_mode == "stream" and args.use_bulk_insert:
            print(f"[INFO] {table_name}: --use-bulk-insert forces full-table mode to parquet")

    use_direct_full_table = args.full_table and full_table_mode == "stream" and not args.skip_load

    try:
        manifest = {}
        if use_direct_full_table:
            run_suffix = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            rows_loaded = stream_full_table_direct(
                table_name,
                schema_entry,
                start_date,
                end_date,
                args,
                conn_manager=conn_manager,
            )
            total_rows = rows_loaded
            manifest = {
                "table": table_name,
                "rows": total_rows,
                "rows_loaded": rows_loaded,
                "parquet": None,
                "mode": "stream",
                "start_date": start_date,
                "end_date": end_date,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            manifest_path = output_dir / f"{table_name.lower()}_{run_suffix}_stream.json"
        else:
            parquet_path, total_rows, rows_loaded = stream_export_and_load(
                table_name,
                schema_entry,
                start_date,
                end_date,
                args,
                conn_manager=conn_manager,
            )
            
            manifest = {
                "table": table_name,
                "rows": total_rows,
                "rows_loaded": rows_loaded,
                "parquet": str(parquet_path),
                "mode": "parquet",
                "start_date": start_date,
                "end_date": end_date,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            manifest_path = parquet_path.with_suffix(".json")

        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"[INFO] Manifest written to {manifest_path}")
        
    except Exception as e:
        print(f"[ERROR] Table {table_name} failed: {e}", file=sys.stderr)
        raise


def save_checkpoint(
    table_name: str,
    job_date: str,
    rows_processed: int,
    last_chunk_id: int,
    status: str,
    checkpoint_data: dict,
    conn_manager: Optional[ConnectionManager] = None,
):
    """Save progress checkpoint to database."""
    if conn_manager and conn_manager.target_conn:
        conn = conn_manager.target_conn
        close_conn = False
    else:
        conn = get_target_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            MERGE dbo.etl_replica_progress AS target
            USING (SELECT ? AS table_name, ? AS job_date) AS source
            ON target.table_name = source.table_name AND target.job_date = source.job_date
            WHEN MATCHED THEN
                UPDATE SET 
                    rows_processed = ?,
                    last_chunk_id = ?,
                    status = ?,
                    checkpoint_data = ?,
                    batch_end = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (table_name, job_date, rows_processed, last_chunk_id, status, checkpoint_data, batch_start)
                VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """, table_name, job_date, rows_processed, last_chunk_id, status, 
              json.dumps(checkpoint_data), table_name, job_date, rows_processed, 
              last_chunk_id, status, json.dumps(checkpoint_data))
        conn.commit()
    finally:
        if close_conn:
            conn.close()


def load_checkpoint(
    table_name: str,
    job_date: str,
    conn_manager: Optional[ConnectionManager] = None,
) -> Optional[dict]:
    """Load progress checkpoint from database."""
    if conn_manager and conn_manager.target_conn:
        conn = conn_manager.target_conn
        close_conn = False
    else:
        conn = get_target_connection()
        close_conn = True
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rows_processed, last_chunk_id, status, checkpoint_data
            FROM dbo.etl_replica_progress
            WHERE table_name = ? AND job_date = ?
        """, table_name, job_date)
        
        row = cursor.fetchone()
        if row:
            return {
                "rows_processed": row[0] or 0,
                "last_chunk_id": row[1] or 0,
                "status": row[2],
                "checkpoint_data": json.loads(row[3]) if row[3] else {},
            }
        return None
    finally:
        if close_conn:
            conn.close()


def table_already_loaded(
    table_name: str,
    start_date: Optional[str],
    end_date: Optional[str],
    full_table: bool,
    conn_manager: Optional[ConnectionManager] = None,
) -> bool:
    """Check if table was already successfully loaded."""
    target_table = f"dbo.com_5013_{table_name}"
    try:
        if conn_manager and conn_manager.target_conn:
            conn = conn_manager.target_conn
            close_conn = False
        else:
            conn = get_target_connection()
            close_conn = True
        
        try:
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
            """, target_table.replace("dbo.", ""))
            
            if cursor.fetchone()[0] == 0:
                return False
            
            # Check if table has data
            if full_table:
                # For full table, just check if any rows exist
                cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
                count = cursor.fetchone()[0]
                return count > 0
            else:
                # For date-filtered, check if data exists for this date range
                date_column = DATE_FILTER_COLUMNS.get(table_name)
                if date_column and start_date:
                    if end_date:
                        cursor.execute(
                            f"SELECT COUNT(*) FROM {target_table} WHERE {date_column} >= ? AND {date_column} < ?",
                            start_date,
                            end_date,
                        )
                    else:
                        cursor.execute(
                            f"SELECT COUNT(*) FROM {target_table} WHERE {date_column} = ?",
                            start_date,
                        )
                    count = cursor.fetchone()[0]
                    return count > 0
            
            return False
        finally:
            if close_conn:
                conn.close()
    except Exception as e:
        # If check fails, assume not loaded
        print(f"[WARN] Could not check if {table_name} is loaded: {e}", file=sys.stderr)
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Xilnex tables to Parquet and load into replica warehouse."
    )
    parser.add_argument(
        "--table",
        action="append",
        help="Specific table(s) to process (e.g., APP_4_SALES). Defaults to all.",
    )
    parser.add_argument("--start-date", help="Start date (inclusive) in YYYY-MM-DD.")
    parser.add_argument(
        "--end-date", help="End date (exclusive) in YYYY-MM-DD. Defaults to start + 1 day."
    )
    parser.add_argument(
        "--output-dir",
        default=config.EXPORT_DIR,
        help="Directory to store Parquet exports (default: %(default)s).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50000,
        help="Row chunk size for streaming exports (default: %(default)s).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Batch size for SQL loading (default: %(default)s).",
    )
    parser.add_argument(
        "--commit-interval",
        type=int,
        default=100000,
        help="Rows per commit interval (default: %(default)s).",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Export only; do not load into SQL Server target.",
    )
    parser.add_argument(
        "--full-table-mode",
        choices=["stream", "parquet"],
        default="stream",
        help="Mode for --full-table loads: stream directly to SQL (default) or write Parquet.",
    )
    parser.add_argument(
        "--full-table",
        action="store_true",
        help="Ignore date filters and export the entire table.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tables that have already been loaded (checks target database).",
    )
    parser.add_argument(
        "--compression",
        choices=["snappy", "gzip", "zstd", "none", "uncompressed"],
        default="snappy",
        help="Parquet compression algorithm (default: snappy).",
    )
    parser.add_argument(
        "--auto-chunk-size",
        action="store_true",
        help="Auto-adjust chunk size based on available memory and column count.",
    )
    parser.add_argument(
        "--use-bulk-insert",
        action="store_true",
        help="Use SQL Server BULK INSERT for faster loading (requires Parquet file accessible to SQL Server).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint if available.",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Process multiple tables in parallel.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Maximum number of parallel workers (default: %(default)s).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    schema = load_schema()

    if args.table:
        tables = args.table
    else:
        tables = list(schema.keys())

    # Filter tables based on --full-table flag
    if args.full_table:
        # Only process reference tables (tables without date columns)
        reference_tables = [t for t in tables if t not in DATE_FILTER_COLUMNS]
        date_based_tables = [t for t in tables if t in DATE_FILTER_COLUMNS]
        
        if date_based_tables:
            print(f"[INFO] --full-table flag used. Skipping date-based tables: {', '.join(date_based_tables)}")
            print(f"[INFO] Use date ranges (--start-date/--end-date) for date-based tables instead.")
            print()
        
        if not reference_tables:
            print("[ERROR] No reference tables to process with --full-table flag.")
            print("[INFO] Reference tables (no date columns):", ", ".join([t for t in schema.keys() if t not in DATE_FILTER_COLUMNS]))
            return
        
        tables = reference_tables
        print(f"[INFO] Processing {len(tables)} reference table(s) with --full-table: {', '.join(tables)}")
        print()

    start_date = args.start_date
    end_date = args.end_date
    if start_date and not end_date:
        # default end date to next day
        end_date = (datetime.fromisoformat(start_date) + timedelta(days=1)).date().isoformat()

    # Filter tables based on date range (if provided without --full-table)
    if start_date and not args.full_table:
        # When date ranges are provided, only process date-based tables
        date_based_tables = [t for t in tables if t in DATE_FILTER_COLUMNS]
        reference_tables = [t for t in tables if t not in DATE_FILTER_COLUMNS]
        
        if reference_tables:
            print(f"[INFO] Date range provided. Skipping reference tables (no date columns): {', '.join(reference_tables)}")
            print(f"[INFO] Use --full-table flag to process reference tables.")
            print()
        
        if not date_based_tables:
            print("[ERROR] No date-based tables to process with date range.")
            print("[INFO] Date-based tables:", ", ".join([t for t in schema.keys() if t in DATE_FILTER_COLUMNS]))
            return
        
        tables = date_based_tables
        print(f"[INFO] Processing {len(tables)} date-based table(s) with date range {start_date} to {end_date}: {', '.join(tables)}")
        print()

    # Auto-adjust chunk size if requested
    if args.auto_chunk_size and tables:
        first_table = tables[0]
        if first_table in schema:
            column_count = len(schema[first_table]["columns"])
            optimal_size = estimate_optimal_chunk_size(column_count)
            if optimal_size != args.chunk_size:
                print(f"[INFO] Auto-adjusted chunk size to {optimal_size:,} based on {column_count} columns")
                args.chunk_size = optimal_size

    # Process tables
    if args.parallel and len(tables) > 1:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {}
            for table in tables:
                entry = schema.get(table)
                if not entry:
                    print(f"[WARN] Table {table} not found in schema, skipping", file=sys.stderr)
                    continue
                
                # Skip logic
                if args.skip_existing and not args.skip_load:
                    if table_already_loaded(table, start_date, end_date, args.full_table):
                        print(f"[SKIP] Table {table} already loaded, skipping")
                        continue
                
                # Submit task
                future = executor.submit(
                    run_for_table, table, entry, args, start_date, end_date, None
                )
                futures[future] = table
            
            # Wait for completion
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"[ERROR] Table {table} failed: {exc}", file=sys.stderr)
    else:
        # Sequential processing with connection reuse
        with ConnectionManager() as conn_manager:
            for table in tables:
                entry = schema.get(table)
                if not entry:
                    print(f"[WARN] Table {table} not found in schema, skipping", file=sys.stderr)
                    continue
                
                # Skip logic
                if args.skip_existing and not args.skip_load:
                    if table_already_loaded(table, start_date, end_date, args.full_table, conn_manager):
                        print(f"[SKIP] Table {table} already loaded, skipping")
                        continue
                
                try:
                    run_for_table(table, entry, args, start_date, end_date, conn_manager)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"[ERROR] Table {table} failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
