"""
Debug script to insert a single row from the latest Parquet export and print
which column triggers numeric errors.

Enhanced with:
- Resume from last checked row (auto-detect from target table or checkpoint file)
- Binary search mode for faster failure detection
- itertuples() for better performance
- Progress checkpoint file for resuming interrupted runs
"""
import argparse
import json
import sys
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


def get_target_table_count(cursor, target_table: str) -> int:
    """Get the number of rows already loaded in the target table."""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def get_checkpoint_row(export_dir: Path) -> int | None:
    """Read the last checked row index from checkpoint file."""
    checkpoint_file = export_dir / ".last_checked_row"
    if checkpoint_file.exists():
        try:
            data = json.loads(checkpoint_file.read_text())
            return data.get("last_checked_row")
        except Exception:
            return None
    return None


def save_checkpoint(export_dir: Path, row_index: int):
    """Save the last checked row index to checkpoint file."""
    checkpoint_file = export_dir / ".last_checked_row"
    checkpoint_file.write_text(json.dumps({"last_checked_row": row_index}))


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
):
    schema = load_schema().get(table_name)
    if not schema:
        raise SystemExit(f"Schema for {table_name} not found")
    columns = [col["name"] for col in schema["columns"]]

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

    # Binary search mode
    if binary_search:
        failure_idx = binary_search_failure(df, columns, cursor, conn, target_table, actual_start, end_row - 1)
        if failure_idx is not None:
            row_series = df.iloc[failure_idx]
            row_data = tuple(row_series[col] for col in columns)
            _, error = test_row_insert(cursor, conn, target_table, columns, row_data)
            print(f"\nFirst failure found at row index {failure_idx}:")
            print(f"Error: {error}")
            for col, value in zip(columns, row_data):
                print(f"  {col}: {value} ({type(value).__name__})")
        else:
            print(f"No failures found between rows {actual_start:,} and {end_row - 1:,}")
        cursor.close()
        conn.close()
        return

    # Linear search mode (faster iteration)
    found_error = False
    rows_checked = 0

    # Iterate over DataFrame subset
    df_subset = df.iloc[actual_start:end_row]

    for i in range(len(df_subset)):
        current_idx = actual_start + i
        rows_checked += 1

        if rows_checked % 100000 == 0:
            print(f"Checked {rows_checked:,} rows (at index {current_idx:,}) without errors...")
            if save_progress:
                save_checkpoint(export_dir, current_idx)

        # Access row as Series (faster than iterrows)
        row_series = df_subset.iloc[i]
        row_data = tuple(row_series[col] for col in columns)
        success, error = test_row_insert(cursor, conn, target_table, columns, row_data)

        if not success:
            print(f"\nError on row index {current_idx}: {error}")
            for col, value in zip(columns, row_data):
                print(f"  {col}: {value} ({type(value).__name__})")
            found_error = True
            if save_progress:
                save_checkpoint(export_dir, current_idx)
            break

    if not found_error:
        print(f"\nNo errors encountered for {rows_checked:,} rows (indices {actual_start:,} to {end_row - 1:,}).")
        if save_progress:
            save_checkpoint(export_dir, end_row - 1)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Debug script to find which row/column causes insert failures",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check first 100k rows
  python scripts/debug_single_row_insert.py APP_4_CUSTOMER --limit 100000

  # Auto-resume from target table or checkpoint
  python scripts/debug_single_row_insert.py APP_4_CUSTOMER --auto-resume

  # Start from specific row
  python scripts/debug_single_row_insert.py APP_4_CUSTOMER --start-row 200000

  # Binary search between rows 200k and 500k
  python scripts/debug_single_row_insert.py APP_4_CUSTOMER --start-row 200000 --limit 300000 --binary-search

  # Full scan with progress saving
  python scripts/debug_single_row_insert.py APP_4_CUSTOMER
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

    args = parser.parse_args()

    main(
        table_name=args.table_name,
        limit=args.limit,
        start_row=args.start_row,
        auto_resume=args.auto_resume,
        binary_search=args.binary_search,
        save_progress=not args.no_save_progress,
    )

