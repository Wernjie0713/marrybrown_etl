import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyodbc

import config

SCHEMA_PATH = Path("docs") / "replica_schema.json"

# Tables that support date filtering and the column to use
DATE_FILTER_COLUMNS = {
    "APP_4_SALES": "DATETIME__SALES_DATE",
    "APP_4_SALESITEM": "DATETIME__SALES_DATE",
    "APP_4_PAYMENT": "DATETIME__DATE",
    "APP_4_VOIDSALESITEM": "DATETIME__VOID_DATETIME",
    "APP_4_SALESCREDITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESCREDITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDELIVERY": "DATETIME__SALES_DATE",
    "APP_4_EPAYMENTLOG": "TRANSACTIONDATETIME",
    "APP_4_VOUCHER": "DATETIME__VOUCHER_DATE",
}


def load_schema() -> Dict[str, dict]:
    data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return {entry["name"]: entry for entry in data["tables"]}


def get_source_connection():
    conn_str = config.build_connection_string(config.AZURE_SQL_CONFIG)
    return pyodbc.connect(conn_str)


def get_target_connection():
    conn_str = config.build_connection_string(config.TARGET_SQL_CONFIG)
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


def export_table(
    table_name: str,
    schema_entry: dict,
    start_date: Optional[str],
    end_date: Optional[str],
    args: argparse.Namespace,
) -> pd.DataFrame:
    query, params = build_select_statement(
        table_name,
        schema_entry,
        start_date,
        end_date,
        args.full_table,
    )
    print(f"\n[EXPORT] {table_name}: running query")
    conn = get_source_connection()
    chunks = []
    total_rows = 0
    for chunk in pd.read_sql_query(
        query, conn, params=params if params else None, chunksize=args.chunk_size
    ):
        chunks.append(chunk)
        total_rows += len(chunk)
        print(f"  fetched {total_rows:,} rows", end="\r", flush=True)
    conn.close()

    if not chunks:
        return pd.DataFrame(columns=[col["name"] for col in schema_entry["columns"]])

    df = pd.concat(chunks, ignore_index=True)
    print(f"  fetched {total_rows:,} rows in total")
    validate_columns(table_name, schema_entry, df.columns)
    return df


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


def write_parquet(
    table_name: str, df: pd.DataFrame, args: argparse.Namespace, suffix: str
) -> Path:
    output_dir = Path(args.output_dir) / table_name.lower()
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{table_name.lower()}_{suffix}.parquet"
    output_path = output_dir / file_name
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"[EXPORT] {table_name}: wrote {len(df):,} rows to {output_path}")
    return output_path


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


def load_into_target(
    table_name: str,
    schema_entry: dict,
    df: pd.DataFrame,
    start_date: Optional[str],
    end_date: Optional[str],
) -> int:
    if df.empty:
        print(f"[LOAD] {table_name}: nothing to load")
        return 0

    target_table = f"dbo.com_5013_{table_name}"
    columns = [col["name"] for col in schema_entry["columns"]]
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)

    conn = get_target_connection()
    cursor = conn.cursor()
    delete_existing_range(
        cursor, target_table, DATE_FILTER_COLUMNS.get(table_name), start_date, end_date
    )
    cursor.fast_executemany = True
    cursor.executemany(
        f"INSERT INTO {target_table} ({column_list}) VALUES ({placeholders})",
        df[columns].itertuples(index=False, name=None),
    )
    conn.commit()
    conn.close()
    print(f"[LOAD] {table_name}: loaded {len(df):,} rows into {target_table}")
    return len(df)


def run_for_table(
    table_name: str,
    schema_entry: dict,
    args: argparse.Namespace,
    start_date: Optional[str],
    end_date: Optional[str],
    suffix: str,
):
    df = export_table(table_name, schema_entry, start_date, end_date, args)
    parquet_path = write_parquet(table_name, df, args, suffix)
    rows_loaded = 0
    if not args.skip_load:
        rows_loaded = load_into_target(table_name, schema_entry, df, start_date, end_date)
    manifest = {
        "table": table_name,
        "rows": len(df),
        "rows_loaded": rows_loaded,
        "parquet": str(parquet_path),
        "start_date": start_date,
        "end_date": end_date,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    manifest_path = parquet_path.with_suffix(".json")
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"[INFO] Manifest written to {manifest_path}")


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
        "--skip-load",
        action="store_true",
        help="Export only; do not load into SQL Server target.",
    )
    parser.add_argument(
        "--full-table",
        action="store_true",
        help="Ignore date filters and export the entire table.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    schema = load_schema()

    if args.table:
        tables = args.table
    else:
        tables = list(schema.keys())

    start_date = args.start_date
    end_date = args.end_date
    if start_date and not end_date:
        # default end date to next day
        end_date = (datetime.fromisoformat(start_date) + timedelta(days=1)).date().isoformat()

    run_suffix = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for table in tables:
        entry = schema.get(table)
        if not entry:
            print(f"[WARN] Table {table} not found in replica_schema.json, skipping", file=sys.stderr)
            continue
        try:
            run_for_table(table, entry, args, start_date, end_date, run_suffix)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[ERROR] Table {table} failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()


