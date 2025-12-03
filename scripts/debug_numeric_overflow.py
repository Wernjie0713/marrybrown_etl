"""Debug script to locate numeric columns that trigger
`Numeric value out of range` (SQLParamData) errors.

Features:
- Reads latest Parquet export for a table
- Runs rows through `prepare_data_for_sql` (same as ETL)
- Attempts inserts row-by-row (resume, limit, binary search supported)
- On failure, reports offending row/values/types and SQL error
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
from scripts.replicate_reference_tables import (  # noqa: E402
    load_schema,
    prepare_data_for_sql,
)


def get_target_table_count(cursor, target_table: str) -> int:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def get_checkpoint_row(export_dir: Path) -> int | None:
    checkpoint_file = export_dir / ".last_numeric_check"
    if checkpoint_file.exists():
        try:
            data = json.loads(checkpoint_file.read_text())
            return data.get("last_checked_row")
        except Exception:
            return None
    return None


def save_checkpoint(export_dir: Path, row_index: int):
    checkpoint_file = export_dir / ".last_numeric_check"
    checkpoint_file.write_text(json.dumps({"last_checked_row": row_index}))


def test_row_insert(cursor, conn, target_table: str, columns: list, row_data: tuple):
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


def inspect_row_failure(row_series: pd.Series) -> str:
    lines = []
    for col, val in row_series.items():
        lines.append(f"  {col}: {val} ({type(val).__name__})")
    return "\n".join(lines)


def binary_search_failure(df, columns, cursor, conn, target_table, start_idx, end_idx, build_row_tuple):
    left, right = start_idx, end_idx
    first_failure = None
    print(f"Binary searching between rows {left:,} and {right:,}...")
    while left <= right:
        mid = (left + right) // 2
        row_series = df.iloc[mid]
        row_data = build_row_tuple(row_series[col] for col in columns)
        success, error = test_row_insert(cursor, conn, target_table, columns, row_data)
        if success:
            left = mid + 1
        else:
            first_failure = (mid, error)
            right = mid - 1
    return first_failure


def main(table_name: str, limit: int | None, start_row: int | None, auto_resume: bool, binary_search: bool):
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
    df = df.astype(object)
    if df.empty:
        raise SystemExit("Parquet file is empty")

    target_table = f"dbo.com_5013_{table_name}"
    conn = pyodbc.connect(
        config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    )
    cursor = conn.cursor()

    actual_start = 0
    if start_row is not None:
        actual_start = start_row
        print(f"Starting from row {actual_start:,} (manual override)")
    elif auto_resume:
        checkpoint_row = get_checkpoint_row(export_dir)
        if checkpoint_row is not None:
            actual_start = checkpoint_row + 1
            print(f"Resuming from checkpoint: row {actual_start:,}")
        else:
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

    end_row = len(df) if limit is None else min(actual_start + limit, len(df))
    print(f"Checking rows {actual_start:,} to {end_row:,} (total: {end_row - actual_start:,} rows)")

    def coerce_python_value(val):
        if val is None or val is pd.NaT:
            return None
        if hasattr(val, "item"):
            val = val.item()
        if isinstance(val, float) and pd.isna(val):
            return None
        return val

    def build_row_tuple(row_iterable):
        return tuple(coerce_python_value(v) for v in row_iterable)

    if binary_search:
        failure = binary_search_failure(
            df,
            columns,
            cursor,
            conn,
            target_table,
            actual_start,
            end_row - 1,
            build_row_tuple,
        )
        if failure is None:
            print(f"No numeric failures between rows {actual_start:,} and {end_row - 1:,}")
        else:
            idx, error = failure
            row_series = df.iloc[idx]
            print(f"\n{'='*80}\nFirst failure at row {idx}\n{'='*80}")
            print(f"SQL error: {error}")
            print("Row values:")
            print(inspect_row_failure(row_series))
        cursor.close()
        conn.close()
        return

    found_error = False
    df_subset = df.iloc[actual_start:end_row]

    for i, row_series in enumerate(df_subset.itertuples(index=False, name=None)):
        current_idx = actual_start + i
        if (i + 1) % 100000 == 0:
            print(f"Checked {i + 1:,} rows (index {current_idx:,})...")
            save_checkpoint(export_dir, current_idx)

        row_data = build_row_tuple(row_series)
        success, error = test_row_insert(cursor, conn, target_table, columns, row_data)
        if not success:
            print(f"\n{'='*80}\nError on row {current_idx}: {error}\n{'='*80}")
            print("Row values:")
            row_series_obj = pd.Series(row_data, index=columns)
            print(inspect_row_failure(row_series_obj))
            save_checkpoint(export_dir, current_idx)
            found_error = True
            break

    if not found_error:
        print(f"\nNo numeric errors encountered for rows {actual_start:,} to {end_row - 1:,}.")
        save_checkpoint(export_dir, end_row - 1)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Debug numeric overflow issues by testing row inserts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/debug_numeric_overflow.py APP_4_CUSTOMER
  python scripts/debug_numeric_overflow.py APP_4_CUSTOMER --limit 200000
  python scripts/debug_numeric_overflow.py APP_4_CUSTOMER --start-row 800000
  python scripts/debug_numeric_overflow.py APP_4_CUSTOMER --binary-search
  python scripts/debug_numeric_overflow.py APP_4_CUSTOMER --auto-resume
        """,
    )
    parser.add_argument("table_name", help="Table name (e.g., APP_4_CUSTOMER)")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to check")
    parser.add_argument("--start-row", type=int, help="Start row index (0-based)")
    parser.add_argument("--auto-resume", action="store_true", help="Resume from checkpoint/target count")
    parser.add_argument("--binary-search", action="store_true", help="Use binary search to find first failure")

    args = parser.parse_args()

    main(
        table_name=args.table_name,
        limit=args.limit,
        start_row=args.start_row,
        auto_resume=args.auto_resume,
        binary_search=args.binary_search,
    )


