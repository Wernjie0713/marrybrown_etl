"""
Monthly parallel replication script with in-memory streaming.

This script streams data month-by-month directly from the source DB to the
target DB, avoiding intermediate Parquet files. Each month is processed by a
separate thread with independent connections. Resume is handled via a simple
checkpoint file that records synced months.

Usage:
    python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31
    python scripts/replicate_monthly_parallel_streaming.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --resume
"""

import argparse
import json
import math
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent directory to path to import config
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pyodbc
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import functions from main ETL script
from replicate_reference_tables import (
    DATE_FILTER_COLUMNS,
    DATETIME_MAX,
    DATETIME_MIN,
    build_row_tuple,
    build_select_statement,
    delete_existing_range,
    get_source_connection,
    get_target_connection,
    load_schema,
    round_to_datetime_precision,
)

import config


class MonthRetryableError(Exception):
    """Raised when a month should be retried due to a transient connection issue."""


def is_connection_lost_error(error: Exception) -> bool:
    """Best-effort detection of transient connection issues that merit a retry."""
    message = " ".join(str(part) for part in getattr(error, "args", [str(error)])).lower()
    transient_tokens = [
        "08s01",  # Communication link failure
        "connection is busy",
        "communication link failure",
        "connection was terminated",
        "connection was closed",
        "socket has been closed",
        "timeout",
        "deadlock",
        "login timeout expired",
        "transport-level error",
        "broken pipe",
        "reset by peer",
    ]
    return any(token in message for token in transient_tokens)


def format_duration(seconds: float) -> str:
    """Format duration in seconds or mXs style."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    return f"{int(minutes)}m{int(secs)}s"


def generate_month_ranges(start_date: str, end_date: str) -> List[Tuple[str, str, str]]:
    """
    Generate list of (month_key, start, end) tuples for each month in range.

    Returns:
        List of tuples: [("2024-01", "2024-01-01", "2024-02-01"), ...]
    """
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    months = []
    current = start

    while current < end:
        month_start = date(current.year, current.month, 1)
        month_end = date(current.year + (current.month // 12), (current.month % 12) + 1, 1)

        month_start = max(month_start, start)
        month_end = min(month_end, end)

        month_key = f"{current.year}-{current.month:02d}"
        months.append((month_key, month_start.isoformat(), month_end.isoformat()))

        current = month_end

    return months


def get_checkpoint_path(table_name: str, output_dir: Path) -> Path:
    """Get path to checkpoint file for this table."""
    return output_dir / f"{table_name.lower()}_monthly_checkpoint.json"


def _convert_datetime_value(val, is_date: bool):
    if val is None:
        return None
    if isinstance(val, (float, int)):
        try:
            if math.isnan(val):
                return None
        except Exception:
            pass

    dt = None
    if isinstance(val, datetime):
        dt = val
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
                dt = datetime.fromisoformat(s_val)
        except Exception:
            return None

    if dt is not None:
        if dt < DATETIME_MIN or dt > DATETIME_MAX:
            return None
        if is_date:
            return dt.date()
        return round_to_datetime_precision(dt)
    return None


def prepare_data_for_sql_polars(pl_df: pl.DataFrame, schema_entry: dict) -> pl.DataFrame:
    """
    Prepare Polars DataFrame for SQL insertion mirroring pandas prepare_data_for_sql.
    """
    if pl_df.is_empty():
        return pl_df

    column_type_map = {col["name"]: col.get("type", "").lower() for col in schema_entry["columns"]}

    exprs = []

    for col_name in pl_df.columns:
        col_type = column_type_map.get(col_name, "")

        if col_type in ("datetime", "date"):
            is_date = col_type == "date"
            date_expr = pl.col(col_name).map_elements(
                lambda v: _convert_datetime_value(v, is_date),
                return_dtype=pl.Object,
                skip_nulls=False,
            )
            exprs.append(date_expr.alias(col_name))
        else:
            # sanitize NaN/NaT -> None and convert scalar objects
            def sanitize(val):
                if val is None:
                    return None
                try:
                    if isinstance(val, float) and math.isnan(val):
                        return None
                except Exception:
                    pass
                if hasattr(val, "item"):
                    try:
                        val = val.item()
                    except Exception:
                        pass
                # Normalize binary-like to bytes
                if isinstance(val, memoryview):
                    val = bytes(val)
                elif isinstance(val, bytearray):
                    val = bytes(val)
                return val

            exprs.append(pl.col(col_name).map_elements(sanitize, return_dtype=pl.Object, skip_nulls=False).alias(col_name))

    cleaned = pl_df.select(exprs)
    return cleaned


def get_nonclustered_indexes(cursor: pyodbc.Cursor, target_table: str) -> List[str]:
    """Return nonclustered index names (not disabled) for a given table."""
    try:
        if "." in target_table:
            schema_name, table_only = target_table.split(".", 1)
        else:
            schema_name, table_only = "dbo", target_table

        cursor.execute(
            """
            SELECT i.name
            FROM sys.indexes i
            JOIN sys.objects o ON i.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ? AND o.name = ? AND i.type_desc = 'NONCLUSTERED' AND i.is_disabled = 0
            """,
            schema_name,
            table_only,
        )
        return [row[0] for row in cursor.fetchall()]
    except Exception as exc:
        print(f"[WARN] Could not list indexes for {target_table}: {exc}", file=sys.stderr)
        return []


def disable_nonclustered_indexes(cursor: pyodbc.Cursor, target_table: str) -> List[str]:
    """Disable nonclustered indexes; return names disabled. Swallows errors and returns empty list on failure."""
    indexes = get_nonclustered_indexes(cursor, target_table)
    if not indexes:
        return []
    disabled = []
    for idx in indexes:
        try:
            cursor.execute(f"ALTER INDEX [{idx}] ON {target_table} DISABLE")
            disabled.append(idx)
        except Exception as exc:
            print(f"[WARN] Could not disable index {idx} on {target_table}: {exc}", file=sys.stderr)
    try:
        cursor.connection.commit()
    except Exception as exc:
        print(f"[WARN] Commit after disabling indexes on {target_table} failed: {exc}", file=sys.stderr)
    return disabled


def rebuild_indexes(cursor: pyodbc.Cursor, target_table: str, indexes: List[str]) -> None:
    """Rebuild the provided indexes; swallows errors to avoid failing the load."""
    if not indexes:
        return
    for idx in indexes:
        try:
            cursor.execute(f"ALTER INDEX [{idx}] ON {target_table} REBUILD")
        except Exception as exc:
            print(f"[WARN] Could not rebuild index {idx} on {target_table}: {exc}", file=sys.stderr)
    try:
        cursor.connection.commit()
    except Exception as exc:
        print(f"[WARN] Commit after rebuilding indexes on {target_table} failed: {exc}", file=sys.stderr)


def load_checkpoint(table_name: str, output_dir: Path) -> Dict:
    """Load checkpoint data if exists."""
    checkpoint_path = get_checkpoint_path(table_name, output_dir)
    if checkpoint_path.exists():
        try:
            raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            return {
                "synced_months": raw.get("synced_months") or raw.get("completed_months", []),
                "failed_months": raw.get("failed_months", []),
                "last_updated": raw.get("last_updated"),
            }
        except Exception:
            return {}
    return {}


def save_checkpoint(
    table_name: str,
    output_dir: Path,
    synced_months: List[str],
    failed_months: List[str],
) -> None:
    """Save checkpoint data."""
    checkpoint_path = get_checkpoint_path(table_name, output_dir)
    checkpoint = {
        "table": table_name,
        "synced_months": synced_months,
        "failed_months": failed_months,
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }
    checkpoint_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")


def stream_month_to_target(
    table_name: str,
    schema_entry: dict,
    month_key: str,
    month_start: str,
    month_end: str,
    chunk_size: int,
    commit_interval: int,
    max_retries: int = 3,
) -> Tuple[str, int]:
    """
    Stream one month of data directly from source to target using in-memory chunks.

    Retries the whole month on transient connection issues to avoid partial duplicates.
    """
    query, params = build_select_statement(
        table_name,
        schema_entry,
        month_start,
        month_end,
        full_table=False,
    )

    columns = [col["name"] for col in schema_entry["columns"]]
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    target_table = f"dbo.com_5013_{table_name}"
    insert_sql = f"INSERT INTO {target_table} WITH (TABLOCK) ({column_list}) VALUES ({placeholders})"

    # Build Polars schema from SQL types
    def map_sql_type_to_polars(sql_type: str) -> pl.DataType:
        t = (sql_type or "").lower()
        if t in ("int", "bigint", "smallint", "tinyint"):
            return pl.Int64
        if t in ("decimal", "numeric", "float", "real", "money"):
            return pl.Float64
        if t in ("date",):
            return pl.Date
        if t in ("datetime", "datetime2", "smalldatetime"):
            return pl.Datetime
        if t in ("bit",):
            return pl.Boolean
        if t in ("timestamp", "binary", "varbinary"):
            return pl.Binary
        # default string
        return pl.Utf8

    polars_schema_base = {
        col["name"]: map_sql_type_to_polars(col.get("type", ""))
        for col in schema_entry["columns"]
    }

    attempt = 1
    while attempt <= max_retries:
        source_conn = None
        target_conn = None
        cursor = None
        disabled_indexes: List[str] = []
        delete_time = disable_time = insert_time = rebuild_time = 0.0
        total_start = time.perf_counter()
        try:
            source_conn = get_source_connection()
            target_conn = get_target_connection()
            cursor = target_conn.cursor()
            cursor.fast_executemany = True

            delete_start = time.perf_counter()
            delete_existing_range(
                cursor,
                target_table,
                DATE_FILTER_COLUMNS.get(table_name),
                month_start,
                month_end,
            )
            target_conn.commit()
            delete_time = time.perf_counter() - delete_start

            try:
                disable_start = time.perf_counter()
                disabled_indexes = disable_nonclustered_indexes(cursor, target_table)
                disable_time = time.perf_counter() - disable_start
                if disabled_indexes:
                    print(f"[INFO] {table_name} {month_key}: disabled indexes {disabled_indexes}")
            except Exception as exc:
                disable_time = time.perf_counter() - disable_start if 'disable_start' in locals() else 0.0
                print(f"[WARN] {table_name} {month_key}: index disable failed, continuing without: {exc}", file=sys.stderr)

            total_loaded = 0
            rows_since_commit = 0

            insert_start = time.perf_counter()
            # Use cursor-based fetch to avoid pandas read_sql; wrap rows in Polars then pandas for existing prep.
            cursor_src = source_conn.cursor()
            cursor_src.execute(query, params or [])
            chunk_idx = 0
            fetched_columns = [desc[0] for desc in cursor_src.description] if cursor_src.description else columns
            # If schema mismatch, prefer actual fetched columns but try to align order to expected when possible.
            if len(fetched_columns) != len(columns):
                print(
                    f"[WARN] {table_name} {month_key}: fetched {len(fetched_columns)} columns but expected {len(columns)}; using fetched schema order",
                    file=sys.stderr,
                )
            selected_columns = columns if set(columns).issubset(set(fetched_columns)) else fetched_columns
            polars_schema = {col: polars_schema_base.get(col, pl.Utf8) for col in fetched_columns}
            while True:
                rows = cursor_src.fetchmany(chunk_size)
                if not rows:
                    break
                # Ensure pyodbc.Row -> tuple for Polars
                rows = [tuple(r) for r in rows]
                # Create Polars DataFrame for downstream prep.
                try:
                    pl_chunk = pl.DataFrame(rows, schema=polars_schema, orient="row")
                except Exception as e:
                    print(f"[DEBUG] {table_name} {month_key}: Polars DataFrame creation failed", file=sys.stderr)
                    print(f"[DEBUG] Error: {e}", file=sys.stderr)
                    print(f"[DEBUG] Fetched columns ({len(fetched_columns)}): {fetched_columns[:10]}...", file=sys.stderr)
                    print(f"[DEBUG] First row sample ({len(rows[0])} values): {rows[0][:10] if rows else 'no rows'}...", file=sys.stderr)
                    print(f"[DEBUG] Expected columns ({len(columns)}): {columns[:10]}...", file=sys.stderr)
                    raise
                if pl_chunk.is_empty():
                    continue
                chunk_pl = prepare_data_for_sql_polars(pl_chunk, schema_entry)
                batch_data = [
                    build_row_tuple(row)
                    for row in chunk_pl.select(selected_columns).rows()
                ]

                try:
                    cursor.executemany(insert_sql, batch_data)
                except Exception as e:
                    if is_connection_lost_error(e):
                        raise MonthRetryableError(
                            f"Connection lost during insert for chunk {chunk_idx}: {e}"
                        ) from e
                    raise

                total_loaded += len(batch_data)
                rows_since_commit += len(batch_data)
                chunk_idx += 1

                if rows_since_commit >= commit_interval:
                    target_conn.commit()
                    rows_since_commit = 0
                    print(
                        f"  [LOAD] {table_name} {month_key}: committed {total_loaded:,} rows",
                        end="\r",
                        flush=True,
                    )

            target_conn.commit()
            insert_time = time.perf_counter() - insert_start
            try:
                rebuild_start = time.perf_counter()
                rebuild_indexes(cursor, target_table, disabled_indexes)
                rebuild_time = time.perf_counter() - rebuild_start
            except Exception as exc:
                rebuild_time = time.perf_counter() - rebuild_start
                print(f"[WARN] {table_name} {month_key}: index rebuild failed: {exc}", file=sys.stderr)
            else:
                disabled_indexes = []

            total_time = time.perf_counter() - total_start
            print(f"[LOAD] {table_name} {month_key}: loaded {total_loaded:,} rows")
            print(
                f"[TIMING] {table_name} {month_key}: "
                f"DELETE {format_duration(delete_time)} | "
                f"DISABLE_IDX {format_duration(disable_time)} | "
                f"INSERT {format_duration(insert_time)} | "
                f"REBUILD_IDX {format_duration(rebuild_time)} | "
                f"TOTAL {format_duration(total_time)}"
            )
            return month_key, total_loaded
        except MonthRetryableError as retry_err:
            attempt += 1
            print(
                f"[WARN] {table_name} {month_key}: {retry_err}. "
                f"Retrying month ({attempt - 1}/{max_retries})...",
                file=sys.stderr,
            )
            time.sleep(min(5, attempt))
            continue
        except pyodbc.Error as err:
            if is_connection_lost_error(err) and attempt < max_retries:
                attempt += 1
                print(
                    f"[WARN] {table_name} {month_key}: transient DB error {err}. "
                    f"Retrying month ({attempt - 1}/{max_retries})...",
                    file=sys.stderr,
                )
                time.sleep(min(5, attempt))
                continue
            raise
        finally:
            if source_conn:
                source_conn.close()
            if target_conn:
                if disabled_indexes and cursor:
                    try:
                        rebuild_indexes(cursor, target_table, disabled_indexes)
                        rebuild_time = time.perf_counter() - rebuild_start if 'rebuild_start' in locals() else rebuild_time
                    except Exception as exc:
                        print(f"[WARN] {table_name} {month_key}: index rebuild during cleanup failed: {exc}", file=sys.stderr)
                target_conn.close()

    raise RuntimeError(f"{table_name} {month_key}: failed after {max_retries} attempts")


def replicate_monthly_parallel(
    table_name: str,
    start_date: str,
    end_date: str,
    output_dir: Path,
    max_workers: int = 12,
    chunk_size: int = 10000,
    resume: bool = False,
    commit_interval: int = 100000,
    max_retries: int = 3,
) -> None:
    """
    Main function: replicate table month-by-month with parallel workers using streaming.
    """
    schema = load_schema()
    if table_name not in schema:
        print(f"[ERROR] Table {table_name} not found in schema", file=sys.stderr)
        return

    if table_name not in DATE_FILTER_COLUMNS:
        print(
            f"[ERROR] Table {table_name} does not support date filtering (not in DATE_FILTER_COLUMNS)",
            file=sys.stderr,
        )
        return

    schema_entry = schema[table_name]
    months = generate_month_ranges(start_date, end_date)
    print(f"[INFO] Processing {len(months)} months for {table_name}")
    print(f"[INFO] Date range: {start_date} to {end_date}")
    print()

    table_output_dir = output_dir / table_name.lower()
    table_output_dir.mkdir(parents=True, exist_ok=True)

    checkpoint = load_checkpoint(table_name, table_output_dir) if resume else {}
    synced_months = set(checkpoint.get("synced_months", []))
    failed_months = set(checkpoint.get("failed_months", []))

    months_to_process = [
        (month_key, month_start, month_end)
        for month_key, month_start, month_end in months
        if month_key not in synced_months
    ]

    if not months_to_process:
        print(f"[INFO] All months already synced for {table_name}")
    else:
        print(f"[INFO] Processing {len(months_to_process)} remaining months")
        print(f"[INFO] Synced months: {sorted(synced_months)}")
        print()

        month_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    stream_month_to_target,
                    table_name,
                    schema_entry,
                    month_key,
                    month_start,
                    month_end,
                    chunk_size,
                    commit_interval,
                    max_retries,
                ): month_key
                for month_key, month_start, month_end in months_to_process
            }

            for future in as_completed(futures):
                month_key = futures[future]
                try:
                    result_month, rows_loaded = future.result()
                    synced_months.add(result_month)
                    if rows_loaded == 0:
                        print(f"[INFO] {table_name} {result_month}: No data for this month")
                    else:
                        print(f"[SYNC] {table_name} {result_month}: {rows_loaded:,} rows streamed")
                except Exception as e:  # pylint: disable=broad-except
                    failed_months.add(month_key)
                    print(f"[ERROR] {table_name} {month_key}: {e}", file=sys.stderr)
                finally:
                    save_checkpoint(
                        table_name,
                        table_output_dir,
                        sorted(synced_months),
                        sorted(failed_months),
                    )

    print(f"\n{'='*70}")
    print(f"[SUMMARY] {table_name}")
    print(f"{'='*70}")
    print(f"  Total months: {len(months)}")
    print(f"  Synced months: {len(synced_months)}/{len(months)}")
    print(f"  Failed months: {len(failed_months)}")
    if synced_months:
        print(f"  Completed: {', '.join(sorted(synced_months))}")
    if failed_months:
        print(f"  Failed: {', '.join(sorted(failed_months))}")
    if not synced_months:
        print("  Status: No months synced yet")
    print(f"{'='*70}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replicate table month-by-month with parallel workers using streaming."
    )
    parser.add_argument(
        "table",
        help="Table name to replicate (e.g., APP_4_SALES)",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (inclusive) in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (exclusive) in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output-dir",
        default=config.EXPORT_DIR,
        help="Directory to store checkpoints (default: %(default)s).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=12,
        help="Maximum number of parallel workers (default: %(default)s).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Row chunk size for streaming exports (default: %(default)s).",
    )
    parser.add_argument(
        "--commit-interval",
        type=int,
        default=100000,
        help="Rows per commit interval (default: %(default)s).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Number of retries per month on transient connection failures (default: %(default)s).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    replicate_monthly_parallel(
        table_name=args.table,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=Path(args.output_dir),
        max_workers=args.max_workers,
        chunk_size=args.chunk_size,
        resume=args.resume,
        commit_interval=args.commit_interval,
        max_retries=args.max_retries,
    )


if __name__ == "__main__":
    main()
