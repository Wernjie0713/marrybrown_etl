"""
Debug script to insert a single row from the latest Parquet export and print
which column triggers numeric errors.
"""
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


def main(table_name: str, limit: int | None = None):
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
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)

    conn = pyodbc.connect(
        config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    )
    cursor = conn.cursor()

    found_error = False
    for idx, row in df.iterrows():
        if limit is not None and idx >= limit:
            break
        if idx and idx % 100000 == 0:
            print(f"Checked {idx:,} rows without errors...")
        try:
            data = tuple(row.get(col) for col in columns)
            cursor.execute(
                f"INSERT INTO {target_table} ({column_list}) VALUES ({placeholders})",
                data,
            )
            conn.rollback()
        except Exception as exc:
            print(f"Error on row index {idx}: {exc}")
            for col, value in zip(columns, data):
                print(f"  {col}: {value} ({type(value)})")
            found_error = True
            break

    if not found_error:
        print("No errors encountered for sampled rows.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python scripts/debug_single_row_insert.py APP_4_ITEM [LIMIT]"
        )
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    main(sys.argv[1], limit=limit)

