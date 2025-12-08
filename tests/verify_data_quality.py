"""
Data quality checks between source Xilnex replica and target cloud warehouse.

Checks:
- Row count comparison (full refresh vs date-filtered tables)
- Random sample comparison (5 rows per table, all columns)
"""

import decimal
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyodbc

# Add project root to import config
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config

# Tables grouped by replication rule
SALES_TABLES = [
    "APP_4_SALES",
    "APP_4_SALESITEM",
    "APP_4_PAYMENT",
    "APP_4_VOIDSALESITEM",
    "APP_4_SALESCREDITNOTE",
    "APP_4_SALESCREDITNOTEITEM",
    "APP_4_SALESDEBITNOTE",
    "APP_4_SALESDEBITNOTEITEM",
    "APP_4_EPAYMENTLOG",
    "APP_4_VOUCHER",
]

REFERENCE_TABLES = [
    "APP_4_ITEM",
    "APP_4_STOCK",
    "APP_4_CUSTOMER",
    "APP_4_POINTRECORD",
    "LOCATION_DETAIL",
    "APP_4_VOUCHER_MASTER",
    "APP_4_CASHIER_DRAWER",
    "APP_4_SALESDELIVERY",
    "APP_4_EXTENDEDSALESITEM",
]

# Date columns for sales tables (same mapping as replicate_reference_tables.py)
DATE_FILTER_COLUMNS = {
    "APP_4_SALES": "DATETIME__SALES_DATE",
    "APP_4_SALESITEM": "DATETIME__SALES_DATE",
    "APP_4_PAYMENT": "DATETIME__DATE",
    "APP_4_VOIDSALESITEM": "DATETIME__VOID_DATETIME",
    "APP_4_SALESCREDITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESCREDITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_EPAYMENTLOG": "TRANSACTIONDATETIME",
    "APP_4_VOUCHER": "DATETIME__VOUCHER_DATE",
}

# Connection strings
SOURCE_CONN_STR = config.build_connection_string(config.AZURE_SQL_CONFIG)
TARGET_CONN_STR = config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)

SOURCE_SCHEMA = "COM_5013"
TARGET_SCHEMA = "dbo"
TARGET_PREFIX = "com_5013_"
START_DATE = "2025-08-01"
END_DATE = "2025-10-31"
SAMPLE_SIZE = 5
NUM_TOLERANCE = decimal.Decimal("0.0001")


def format_int(value: int) -> str:
    return f"{value:,}"


def quote_table(schema: str, table: str) -> str:
    return f"[{schema}].[{table}]"


def get_key_column(cursor: pyodbc.Cursor, schema: str, table: str) -> str:
    """Pick a key column, preferring ID, then LOCATION_INT_ID for LOCATION_DETAIL, else first column."""
    if table.upper() == "LOCATION_DETAIL":
        return "LOCATION_INT_ID"

    columns = []
    for col in cursor.columns(table=table, schema=schema):
        columns.append(col.column_name)

    preferred = ["ID", f"{table}_ID", "LOCATION_INT_ID"]
    for candidate in preferred:
        if candidate in columns:
            return candidate

    return columns[0] if columns else "ID"


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def values_match(source: Any, target: Any) -> bool:
    if source is None and target is None:
        return True
    if isinstance(source, decimal.Decimal) or isinstance(target, decimal.Decimal):
        try:
            s_dec = source if isinstance(source, decimal.Decimal) else decimal.Decimal(str(source))
            t_dec = target if isinstance(target, decimal.Decimal) else decimal.Decimal(str(target))
            return abs(s_dec - t_dec) <= NUM_TOLERANCE
        except Exception:
            return source == target
    return source == target


def fetch_row(cursor: pyodbc.Cursor, table_sql: str, key_col: str, key_value: Any) -> Optional[Dict[str, Any]]:
    cursor.execute(f"SELECT * FROM {table_sql} WHERE [{key_col}] = ?", key_value)
    row = cursor.fetchone()
    if not row:
        return None
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row))


def random_keys(
    cursor: pyodbc.Cursor,
    table_sql: str,
    key_col: str,
    date_column: Optional[str],
) -> List[Any]:
    where_clause = ""
    params: List[Any] = []
    if date_column:
        where_clause = f" WHERE [{date_column}] >= ? AND [{date_column}] <= ?"
        params.extend([START_DATE, END_DATE])
    query = f"SELECT TOP {SAMPLE_SIZE} [{key_col}] FROM {table_sql}{where_clause} ORDER BY NEWID()"
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]


def compare_counts(
    source_cursor: pyodbc.Cursor,
    target_cursor: pyodbc.Cursor,
    table: str,
    date_column: Optional[str],
) -> Tuple[int, int]:
    source_table = quote_table(SOURCE_SCHEMA, table)
    target_table = quote_table(TARGET_SCHEMA, f"{TARGET_PREFIX}{table}")

    source_where = ""
    target_where = ""
    params = []
    if date_column:
        source_where = f" WHERE [{date_column}] >= ? AND [{date_column}] <= ?"
        target_where = f" WHERE [{date_column}] >= ? AND [{date_column}] <= ?"
        params = [START_DATE, END_DATE]

    if params:
        source_cursor.execute(f"SELECT COUNT(*) FROM {source_table}{source_where}", params)
    else:
        source_cursor.execute(f"SELECT COUNT(*) FROM {source_table}{source_where}")
    source_count = source_cursor.fetchone()[0]

    if params:
        target_cursor.execute(f"SELECT COUNT(*) FROM {target_table}{target_where}", params)
    else:
        target_cursor.execute(f"SELECT COUNT(*) FROM {target_table}{target_where}")
    target_count = target_cursor.fetchone()[0]

    return source_count, target_count


def compare_samples(
    source_cursor: pyodbc.Cursor,
    target_cursor: pyodbc.Cursor,
    table: str,
    date_column: Optional[str],
) -> Tuple[int, List[str]]:
    source_table = quote_table(SOURCE_SCHEMA, table)
    target_table = quote_table(TARGET_SCHEMA, f"{TARGET_PREFIX}{table}")

    key_col = get_key_column(source_cursor, SOURCE_SCHEMA, table)
    keys = random_keys(source_cursor, source_table, key_col, date_column)

    matches = 0
    mismatches: List[str] = []

    for key_val in keys:
        source_row = fetch_row(source_cursor, source_table, key_col, key_val)
        target_row = fetch_row(target_cursor, target_table, key_col, key_val)

        if source_row is None:
            mismatches.append(f"- {table}: key {key_val} missing in source")
            continue
        if target_row is None:
            mismatches.append(f"- {table}: key {key_val} missing in target")
            continue

        row_match = True
        for col, source_val in source_row.items():
            target_val = target_row.get(col)
            norm_source = normalize_value(source_val)
            norm_target = normalize_value(target_val)
            if not values_match(norm_source, norm_target):
                row_match = False
                mismatches.append(
                    f"- {table}, key {key_val}, column {col}: source={norm_source!r} vs target={norm_target!r}"
                )
        if row_match:
            matches += 1

    return matches, mismatches


def main():
    total_tables = len(SALES_TABLES) + len(REFERENCE_TABLES)
    passed_tables = 0

    print("=" * 60)
    print("DATA QUALITY CHECK: Source (Xilnex) vs Target (Cloud)")
    print("=" * 60)
    print("\nROW COUNT COMPARISON")
    print("-" * 64)
    print(f"{'Table':<30} {'Source':>12} {'Target':>12} {'Status':>8} {'Diff':>6}")
    print("-" * 64)

    with pyodbc.connect(SOURCE_CONN_STR, timeout=30) as source_conn, pyodbc.connect(
        TARGET_CONN_STR, timeout=30
    ) as target_conn:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        mismatch_details: List[str] = []
        sample_summary: List[str] = []

        # Row count comparison
        for table in REFERENCE_TABLES + SALES_TABLES:
            date_column = DATE_FILTER_COLUMNS.get(table)
            source_count, target_count = compare_counts(source_cursor, target_cursor, table, date_column)
            match = source_count == target_count
            status = "MATCH" if match else "MISMATCH"
            diff = target_count - source_count
            print(
                f"{table:<30} {format_int(source_count):>12} {format_int(target_count):>12} {status:>8} {diff:>6}"
            )
            if match:
                passed_tables += 1

        print("-" * 64)

        # Random sample comparison
        print("\nRANDOM SAMPLE COMPARISON (5 rows per table)")
        print("-" * 64)
        for table in REFERENCE_TABLES + SALES_TABLES:
            date_column = DATE_FILTER_COLUMNS.get(table)
            try:
                matches, mismatches = compare_samples(source_cursor, target_cursor, table, date_column)
                if mismatches:
                    sample_summary.append(f"FAIL {table}: {matches}/{SAMPLE_SIZE} rows match")
                    mismatch_details.extend(mismatches)
                else:
                    sample_summary.append(f"OK   {table}: {matches}/{SAMPLE_SIZE} rows match")
            except Exception as exc:  # pylint: disable=broad-except
                sample_summary.append(f"FAIL {table}: error during comparison ({exc})")

        for line in sample_summary:
            print(line)
        for line in mismatch_details:
            print(f"  {line}")

    print("\n" + "=" * 60)
    print(f"SUMMARY: {passed_tables}/{total_tables} tables passed row count check")
    print("=" * 60)


if __name__ == "__main__":
    main()
