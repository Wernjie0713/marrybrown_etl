"""
Debug script to find datetime values that are out of SQL Server DATETIME range.

SQL Server DATETIME constraints:
- Range: 1753-01-01 00:00:00 to 9999-12-31 23:59:59
- Precision: Rounded to .000, .003, or .007 seconds (no microseconds)

Enhanced with:
- Resume from last checked row (auto-detect from target table or checkpoint file)
- Binary search mode for faster failure detection
- Pre-validation of datetime values before insert attempt
- Progress checkpoint file for resuming interrupted runs
- Detailed reporting of out-of-range datetime values
"""
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyodbc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from scripts.export_and_load_replica import (
    load_schema,
    prepare_data_for_sql,
)  # noqa: E402

# SQL Server DATETIME range limits
DATETIME_MIN = datetime(1753, 1, 1, 0, 0, 0)
DATETIME_MAX = datetime(9999, 12, 31, 23, 59, 59)


def get_target_table_count(cursor, target_table: str) -> int:
    """Get the number of rows already loaded in the target table."""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def get_checkpoint_row(export_dir: Path) -> int | None:
    """Read the last checked row index from checkpoint file."""
    checkpoint_file = export_dir / ".last_datetime_check"
    if checkpoint_file.exists():
        try:
            data = json.loads(checkpoint_file.read_text())
            return data.get("last_checked_row")
        except Exception:
            return None
    return None


def save_checkpoint(export_dir: Path, row_index: int):
    """Save the last checked row index to checkpoint file."""
    checkpoint_file = export_dir / ".last_datetime_check"
    checkpoint_file.write_text(json.dumps({"last_checked_row": row_index}))


def validate_datetime_value(val, col_name: str, col_type: str) -> tuple[bool, str | None]:
    """
    Validate if a datetime value is within SQL Server DATETIME range.
    Returns (is_valid, error_message).
    """
    if val is None:
        return True, None
    
    # For DATE columns, convert to datetime for range check
    if isinstance(val, date) and not isinstance(val, datetime):
        dt = datetime.combine(val, datetime.min.time())
    elif isinstance(val, datetime):
        dt = val
    elif isinstance(val, pd.Timestamp):
        dt = val.to_pydatetime()
    else:
        # Not a datetime/date object, skip validation
        return True, None
    
    # Check range
    if dt < DATETIME_MIN:
        return False, f"Date {dt} is before DATETIME minimum ({DATETIME_MIN})"
    if dt > DATETIME_MAX:
        return False, f"Date {dt} is after DATETIME maximum ({DATETIME_MAX})"
    
    # Check precision (DATETIME rounds to .000/.003/.007 seconds)
    # Microseconds should be handled, but we'll note if precision is too fine
    if dt.microsecond > 0 and dt.microsecond % 1000 != 0:
        # Has microseconds that aren't multiples of milliseconds
        # This will be rounded by SQL Server, but might cause issues
        return True, f"Warning: Has microseconds ({dt.microsecond}) that will be rounded"
    
    return True, None


def find_datetime_columns(schema_entry: dict) -> dict[str, str]:
    """Find all datetime/date columns and their types from schema."""
    datetime_cols = {}
    for col in schema_entry["columns"]:
        col_type = col.get("type", "").lower()
        if col_type in ("datetime", "date"):
            datetime_cols[col["name"]] = col_type
    return datetime_cols


def test_row_insert(cursor, conn, target_table: str, columns: list, row_data: tuple) -> tuple[bool, Exception | None]:
    """Test inserting a single row. Returns (success, error)."""
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    try:
        cursor.execute(
            f"INSERT INTO {target_table} ({column_list}) VALUES ({placeholders})",
            row_data,
        )
        conn.rollback()
        return True, None
    except Exception as exc:
        return False, exc


def binary_search_failure(
    df: pd.DataFrame,
    columns: list,
    datetime_cols: dict,
    cursor,
    conn,
    target_table: str,
    start_idx: int,
    end_idx: int,
) -> int | None:
    """Binary search to find the first failing row between start_idx and end_idx."""
    left, right = start_idx, end_idx
    first_failure = None

    print(f"Binary searching between rows {left:,} and {right:,}...")

    while left <= right:
        mid = (left + right) // 2
        row_series = df.iloc[mid]
        row_data = tuple(row_series[col] for col in columns)

        # Pre-validate datetime values
        datetime_issues = []
        for col_name, col_type in datetime_cols.items():
            if col_name in columns:
                col_idx = columns.index(col_name)
                val = row_data[col_idx]
                is_valid, error_msg = validate_datetime_value(val, col_name, col_type)
                if not is_valid:
                    datetime_issues.append(f"{col_name}: {error_msg}")

        # If datetime validation fails, this is the problem row
        if datetime_issues:
            first_failure = mid
            right = mid - 1
            continue

        # Try actual insert
        success, error = test_row_insert(cursor, conn, target_table, columns, row_data)
        if success:
            # This row works, failure must be later
            left = mid + 1
        else:
            # This row fails, failure might be earlier or this is it
            first_failure = mid
            right = mid - 1

    return first_failure


def main(
    table_name: str,
    limit: int | None = None,
    start_row: int | None = None,
    auto_resume: bool = False,
    binary_search: bool = False,
    save_progress: bool = True,
    validate_only: bool = False,
):
    schema = load_schema().get(table_name)
    if not schema:
        raise SystemExit(f"Schema for {table_name} not found")
    columns = [col["name"] for col in schema["columns"]]
    
    # Find datetime columns
    datetime_cols = find_datetime_columns(schema)
    if not datetime_cols:
        print(f"No datetime/date columns found in {table_name}")
        return
    
    print(f"Found {len(datetime_cols)} datetime/date column(s): {', '.join(datetime_cols.keys())}")

    export_dir = PROJECT_ROOT / "exports" / table_name.lower()
    parquet_files = sorted(export_dir.glob("*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No parquet files in {export_dir}")
    parquet_path = parquet_files[-1]
    print(f"Using {parquet_path}")

    df = pd.read_parquet(parquet_path)
    df = prepare_data_for_sql(df, schema)
    if df.empty:
        raise SystemExit("Parquet file is empty")

    df = df.where(pd.notnull(df), None)
    target_table = f"dbo.com_5013_{table_name}"

    conn = pyodbc.connect(
        config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    )
    cursor = conn.cursor()

    # Determine start row
    actual_start = 0
    if start_row is not None:
        actual_start = start_row
        print(f"Starting from row {actual_start:,} (manual override)")
    elif auto_resume:
        # Try checkpoint file first
        checkpoint_row = get_checkpoint_row(export_dir)
        if checkpoint_row is not None:
            actual_start = checkpoint_row + 1
            print(f"Resuming from checkpoint: row {actual_start:,}")
        else:
            # Fall back to target table count
            table_count = get_target_table_count(cursor, target_table)
            if table_count > 0:
                actual_start = table_count
                print(f"Auto-resuming from target table count: row {actual_start:,}")
            else:
                print("No checkpoint or existing data found, starting from row 0")

    if actual_start >= len(df):
        print(f"Start row {actual_start:,} exceeds DataFrame length ({len(df):,}). Nothing to check.")
        cursor.close()
        conn.close()
        return

    # Determine end row
    end_row = len(df) if limit is None else min(actual_start + limit, len(df))
    print(f"Checking rows {actual_start:,} to {end_row:,} (total: {end_row - actual_start:,} rows)")
    print(f"DATETIME range: {DATETIME_MIN} to {DATETIME_MAX}\n")

    # Binary search mode
    if binary_search:
        failure_idx = binary_search_failure(
            df, columns, datetime_cols, cursor, conn, target_table, actual_start, end_row - 1
        )
        if failure_idx is not None:
            row_series = df.iloc[failure_idx]
            row_data = tuple(row_series[col] for col in columns)
            
            print(f"\n{'='*80}")
            print(f"First failure found at row index {failure_idx}:")
            print(f"{'='*80}")
            
            # Check datetime validation
            datetime_issues = []
            for col_name, col_type in datetime_cols.items():
                if col_name in columns:
                    col_idx = columns.index(col_name)
                    val = row_data[col_idx]
                    is_valid, error_msg = validate_datetime_value(val, col_name, col_type)
                    if not is_valid:
                        datetime_issues.append(f"{col_name}: {error_msg}")
                    elif error_msg:  # Warning
                        datetime_issues.append(f"{col_name}: {error_msg}")
            
            if datetime_issues:
                print("\nDatetime validation issues:")
                for issue in datetime_issues:
                    print(f"  ❌ {issue}")
            
            # Try actual insert to see SQL Server error
            _, error = test_row_insert(cursor, conn, target_table, columns, row_data)
            print(f"\nSQL Server error: {error}")
            
            print(f"\nRow data (showing datetime columns):")
            for col_name in datetime_cols.keys():
                if col_name in columns:
                    col_idx = columns.index(col_name)
                    val = row_data[col_idx]
                    print(f"  {col_name}: {val} ({type(val).__name__})")
        else:
            print(f"No failures found between rows {actual_start:,} and {end_row - 1:,}")
        cursor.close()
        conn.close()
        return

    # Linear search mode
    found_error = False
    rows_checked = 0
    datetime_issues_found = []

    # Iterate over DataFrame subset
    df_subset = df.iloc[actual_start:end_row]

    for i in range(len(df_subset)):
        current_idx = actual_start + i
        rows_checked += 1

        if rows_checked % 100000 == 0:
            print(f"Checked {rows_checked:,} rows (at index {current_idx:,}) without errors...")
            if save_progress:
                save_checkpoint(export_dir, current_idx)

        # Access row as Series
        row_series = df_subset.iloc[i]
        row_data = tuple(row_series[col] for col in columns)

        # Pre-validate datetime values
        row_datetime_issues = []
        for col_name, col_type in datetime_cols.items():
            if col_name in columns:
                col_idx = columns.index(col_name)
                val = row_data[col_idx]
                is_valid, error_msg = validate_datetime_value(val, col_name, col_type)
                if not is_valid:
                    row_datetime_issues.append((col_name, error_msg, val))

        if row_datetime_issues:
            found_error = True
            datetime_issues_found.append((current_idx, row_datetime_issues))
            print(f"\n{'='*80}")
            print(f"❌ Row {current_idx}: Out-of-range datetime values found")
            print(f"{'='*80}")
            for col_name, error_msg, val in row_datetime_issues:
                print(f"  {col_name}: {val}")
                print(f"    Error: {error_msg}")
            
            if not validate_only:
                # Try actual insert to see SQL Server error
                _, error = test_row_insert(cursor, conn, target_table, columns, row_data)
                print(f"\n  SQL Server error: {error}")
            
            if save_progress:
                save_checkpoint(export_dir, current_idx)
            
            # Stop after first error unless --continue flag is added
            break

        # If validate_only, skip actual insert test
        if validate_only:
            continue

        # Try actual insert
        success, error = test_row_insert(cursor, conn, target_table, columns, row_data)
        if not success:
            # Check if it's a datetime error
            error_str = str(error)
            if "datetime" in error_str.lower() or "date" in error_str.lower():
                found_error = True
                print(f"\n{'='*80}")
                print(f"❌ Row {current_idx}: SQL Server datetime error")
                print(f"{'='*80}")
                print(f"Error: {error}")
                
                # Show datetime values for this row
                print(f"\nDatetime values in this row:")
                for col_name in datetime_cols.keys():
                    if col_name in columns:
                        col_idx = columns.index(col_name)
                        val = row_data[col_idx]
                        is_valid, error_msg = validate_datetime_value(val, col_name, datetime_cols[col_name])
                        status = "✅" if is_valid and not error_msg else "❌"
                        print(f"  {status} {col_name}: {val} ({type(val).__name__})")
                        if error_msg:
                            print(f"      {error_msg}")
                
                if save_progress:
                    save_checkpoint(export_dir, current_idx)
                break

    if not found_error:
        print(f"\n✅ No datetime range issues found in {rows_checked:,} rows (indices {actual_start:,} to {end_row - 1:,}).")
        if save_progress:
            save_checkpoint(export_dir, end_row - 1)
    else:
        print(f"\n{'='*80}")
        print(f"Summary: Found {len(datetime_issues_found)} row(s) with datetime range issues")
        print(f"{'='*80}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Debug script to find datetime values out of SQL Server DATETIME range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check first 100k rows
  python scripts/debug_datetime_range.py APP_4_CUSTOMER --limit 100000

  # Auto-resume from target table or checkpoint
  python scripts/debug_datetime_range.py APP_4_CUSTOMER --auto-resume

  # Binary search between rows 200k and 500k
  python scripts/debug_datetime_range.py APP_4_CUSTOMER --start-row 200000 --limit 300000 --binary-search

  # Validate only (don't test actual inserts)
  python scripts/debug_datetime_range.py APP_4_CUSTOMER --validate-only

  # Full scan with progress saving
  python scripts/debug_datetime_range.py APP_4_CUSTOMER
        """,
    )
    parser.add_argument("table_name", help="Table name (e.g., APP_4_CUSTOMER)")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to check (default: all)")
    parser.add_argument("--start-row", type=int, help="Start from this row index (0-based)")
    parser.add_argument(
        "--auto-resume",
        action="store_true",
        help="Auto-resume from checkpoint file or target table count",
    )
    parser.add_argument(
        "--binary-search",
        action="store_true",
        help="Use binary search to find first failure (faster when failure is known to exist)",
    )
    parser.add_argument(
        "--no-save-progress",
        action="store_true",
        help="Don't save progress checkpoint file",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate datetime ranges, don't test actual SQL inserts",
    )

    args = parser.parse_args()

    main(
        table_name=args.table_name,
        limit=args.limit,
        start_row=args.start_row,
        auto_resume=args.auto_resume,
        binary_search=args.binary_search,
        save_progress=not args.no_save_progress,
        validate_only=args.validate_only,
    )

