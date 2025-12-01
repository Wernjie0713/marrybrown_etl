"""
Monthly parallel replication script with resume capability.

This script replicates a table month-by-month using parallel workers.
Each worker processes one month, writes to temporary Parquet files,
then merges into final Parquet. On resume, only complete months are loaded.

Usage:
    python scripts/replicate_monthly_parallel.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31
    python scripts/replicate_monthly_parallel.py APP_4_SALES --start-date 2024-01-01 --end-date 2024-12-31 --resume
"""

import argparse
import json
import sys
import threading
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent directory to path to import config
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import functions from main ETL script
from export_and_load_replica import (
    build_select_statement,
    coerce_python_value,
    build_row_tuple,
    get_source_connection,
    get_target_connection,
    load_schema,
    prepare_data_for_sql,
    DATE_FILTER_COLUMNS,
    COMPRESSION_MAP,
    ConnectionManager,
    delete_existing_range,
)

import config

# Lock for thread-safe Parquet merging
merge_lock = threading.Lock()


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
        # Get first day of current month
        month_start = date(current.year, current.month, 1)
        
        # Get first day of next month (exclusive end)
        if current.month == 12:
            month_end = date(current.year + 1, 1, 1)
        else:
            month_end = date(current.year, current.month + 1, 1)
        
        # Clamp to user's date range
        month_start = max(month_start, start)
        month_end = min(month_end, end)
        
        month_key = f"{current.year}-{current.month:02d}"
        months.append((month_key, month_start.isoformat(), month_end.isoformat()))
        
        # Move to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    
    return months


def export_month(
    table_name: str,
    schema_entry: dict,
    month_key: str,
    month_start: str,
    month_end: str,
    output_dir: Path,
    chunk_size: int = 50000,
    compression: str = "snappy",
) -> Tuple[str, int, Optional[Path]]:
    """
    Export one month of data to a temporary Parquet file.
    
    Returns:
        (month_key, rows_exported, parquet_path) or (month_key, 0, None) on error
    """
    temp_parquet = output_dir / f"{table_name.lower()}_{month_key}_temp.parquet"
    
    try:
        # Build query for this month
        query, params = build_select_statement(
            table_name,
            schema_entry,
            month_start,
            month_end,
            full_table=False,
        )
        
        # Connect and export
        source_conn = get_source_connection()
        try:
            total_rows = 0
            parquet_writer = None
            schema = None
            chunk_idx = 0
            
            chunk_iter = pd.read_sql_query(
                query, source_conn, params=params if params else None, chunksize=chunk_size
            )
            
            for chunk in chunk_iter:
                if chunk.empty:
                    continue
                
                # Prepare data for SQL (same logic as main script)
                chunk = prepare_data_for_sql(chunk, schema_entry)
                
                # Convert to PyArrow Table
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                
                # Handle schema initialization and unification
                if schema is None:
                    # First chunk - fix null types
                    fields = []
                    for field in table.schema:
                        if pa.types.is_null(field.type):
                            fields.append(pa.field(field.name, pa.string(), nullable=True))
                        else:
                            fields.append(field)
                    
                    if fields != list(table.schema):
                        table = table.cast(pa.schema(fields), safe=False)
                    
                    schema = table.schema
                    parquet_writer = pq.ParquetWriter(
                        temp_parquet,
                        schema,
                        compression=COMPRESSION_MAP.get(compression, "snappy"),
                        use_dictionary=True,
                    )
                else:
                    # Unify schemas if needed
                    if schema != table.schema:
                        try:
                            unified_schema = pa.unify_schemas([schema, table.schema])
                            if unified_schema != schema:
                                parquet_writer.close()
                                schema = unified_schema
                                parquet_writer = pq.ParquetWriter(
                                    temp_parquet,
                                    schema,
                                    compression=COMPRESSION_MAP.get(compression, "snappy"),
                                    use_dictionary=True,
                                )
                            table = table.cast(schema, safe=False)
                        except Exception:
                            table = table.cast(schema, safe=False)
                
                # Write chunk
                parquet_writer.write_table(table)
                total_rows += len(chunk)
                chunk_idx += 1
            
            if parquet_writer:
                parquet_writer.close()
            
            if total_rows == 0:
                # Empty month - remove temp file
                if temp_parquet.exists():
                    temp_parquet.unlink()
                return (month_key, 0, None)
            
            return (month_key, total_rows, temp_parquet)
            
        finally:
            source_conn.close()
            
    except Exception as e:
        print(f"[ERROR] {table_name} {month_key}: Export failed: {e}", file=sys.stderr)
        # Clean up partial file
        if temp_parquet.exists():
            temp_parquet.unlink()
        return (month_key, 0, None)


def merge_month_parquets(
    table_name: str,
    month_parquets: List[Tuple[str, Path]],
    final_parquet: Path,
    compression: str = "snappy",
    append_mode: bool = False,
) -> int:
    """
    Merge multiple month Parquet files into final Parquet file.
    Thread-safe: uses lock to prevent concurrent merges.

    This optimized version:
    - Reads Parquet files directly into PyArrow tables (no pandas conversion).
    - Unifies schemas once, casts tables, then concatenates in memory.
    - Writes the merged table in a single Parquet write (much faster).
    """
    with merge_lock:
        tables_to_merge: List[pa.Table] = []

        # If appending, read existing data first
        if append_mode and final_parquet.exists():
            try:
                print(f"  [MERGE] Reading existing Parquet for append...", flush=True)
                existing_table = pq.read_table(final_parquet)
                if len(existing_table):
                    tables_to_merge.append(existing_table)
                    print(f"  [MERGE] Existing rows: {len(existing_table):,}")
            except Exception as e:
                print(f"[WARN] Could not read existing Parquet for append: {e}", file=sys.stderr)

        # Read all month Parquets
        for month_key, month_parquet in month_parquets:
            if not month_parquet or not month_parquet.exists():
                continue
            try:
                print(f"  [MERGE] Reading {month_key} ({month_parquet.name})...", flush=True)
                month_table = pq.read_table(month_parquet)
                if len(month_table):
                    tables_to_merge.append(month_table)
                    print(f"    [MERGE] {month_key}: {len(month_table):,} rows")
                else:
                    print(f"    [MERGE] {month_key}: empty, skipping")
            except Exception as e:
                print(f"[ERROR] Failed to read {month_parquet}: {e}", file=sys.stderr)

        if not tables_to_merge:
            print("[WARN] No month tables to merge")
            return 0

        # Unify schemas
        print(f"  [MERGE] Unifying schemas for {len(tables_to_merge)} tables...", flush=True)
        unified_schema = pa.unify_schemas([table.schema for table in tables_to_merge])

        # Cast all tables to unified schema
        unified_tables = []
        for table in tables_to_merge:
            if table.schema != unified_schema:
                unified_tables.append(table.cast(unified_schema, safe=False))
            else:
                unified_tables.append(table)

        # Concatenate tables
        print("  [MERGE] Concatenating tables...", flush=True)
        merged_table = pa.concat_tables(unified_tables, promote=False)
        total_rows = len(merged_table)
        print(f"  [MERGE] Total rows after concat: {total_rows:,}")

        # Write merged Parquet
        temp_final = final_parquet.with_suffix(".tmp.parquet")
        if temp_final.exists():
            temp_final.unlink()

        print(f"  [MERGE] Writing merged Parquet to {temp_final}...", flush=True)
        pq.write_table(
            merged_table,
            temp_final,
            compression=COMPRESSION_MAP.get(compression, "snappy"),
            use_dictionary=True,
        )

        # Replace old file
        if final_parquet.exists():
            final_parquet.unlink()
        temp_final.rename(final_parquet)
        print(f"  [MERGE] Final Parquet ready: {final_parquet} ({total_rows:,} rows)")

        # Clean up temp month files
        for _, month_parquet in month_parquets:
            if month_parquet and month_parquet.exists():
                month_parquet.unlink()

        return total_rows


def get_checkpoint_path(table_name: str, output_dir: Path) -> Path:
    """Get path to checkpoint file for this table."""
    return output_dir / f"{table_name.lower()}_monthly_checkpoint.json"


def load_checkpoint(table_name: str, output_dir: Path) -> Dict:
    """Load checkpoint data if exists."""
    checkpoint_path = get_checkpoint_path(table_name, output_dir)
    if checkpoint_path.exists():
        try:
            return json.loads(checkpoint_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_checkpoint(
    table_name: str,
    output_dir: Path,
    completed_months: List[str],
    failed_months: List[str],
    final_parquet: Optional[Path],
) -> None:
    """Save checkpoint data."""
    checkpoint_path = get_checkpoint_path(table_name, output_dir)
    checkpoint = {
        "table": table_name,
        "completed_months": completed_months,
        "failed_months": failed_months,
        "final_parquet": str(final_parquet) if final_parquet else None,
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }
    checkpoint_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")


def get_complete_months_from_parquet(parquet_path: Path, date_column: str, months: List[Tuple[str, str, str]]) -> List[str]:
    """
    Analyze Parquet file to determine which months are complete.
    Only returns months where all data for that month exists (month end date is covered).
    """
    if not parquet_path.exists():
        return []
    
    try:
        parquet_file = pq.ParquetFile(parquet_path)
        
        # Read date column to find max date
        # Sample from multiple batches to get accurate max
        max_date_found = None
        
        for batch in parquet_file.iter_batches(batch_size=100000, columns=[date_column]):
            df = batch.to_pandas()
            if date_column not in df.columns or df.empty:
                continue
            
            # Convert date column to dates
            date_series = df[date_column].dropna()
            if date_series.empty:
                continue
            
            # Convert to date objects
            dates = []
            for val in date_series:
                if isinstance(val, (datetime, pd.Timestamp)):
                    dates.append(val.date() if hasattr(val, 'date') else val.to_pydatetime().date())
                elif isinstance(val, date):
                    dates.append(val)
                elif isinstance(val, str):
                    try:
                        dates.append(datetime.fromisoformat(val[:10]).date())
                    except Exception:
                        pass
            
            if dates:
                batch_max = max(dates)
                if max_date_found is None or batch_max > max_date_found:
                    max_date_found = batch_max
        
        if max_date_found is None:
            return []
        
        # Find complete months
        # A month is complete only if max_date >= month_end (we have all data for that month)
        # If max_date falls within a month, that month is incomplete
        complete_months = []
        for month_key, month_start, month_end in months:
            month_start_date = datetime.fromisoformat(month_start).date()
            month_end_date = datetime.fromisoformat(month_end).date()
            
            # Month is complete if:
            # 1. max_date >= month_end (we have data up to or past the month end)
            # 2. This means we have all data for that month
            if max_date_found >= month_end_date:
                complete_months.append(month_key)
            # If max_date is within this month (month_start <= max_date < month_end),
            # this month is incomplete, so stop here
            elif month_start_date <= max_date_found < month_end_date:
                break
        
        return complete_months
        
    except Exception as e:
        print(f"[WARN] Could not analyze Parquet for complete months: {e}", file=sys.stderr)
        return []


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
    """Load from Parquet file in streaming fashion (reused from main script)."""
    from export_and_load_replica import load_from_parquet_streaming as base_load
    
    return base_load(
        table_name,
        schema_entry,
        parquet_path,
        start_date,
        end_date,
        batch_size,
        commit_interval,
        conn_manager,
    )


def replicate_monthly_parallel(
    table_name: str,
    start_date: str,
    end_date: str,
    output_dir: Path,
    max_workers: int = 12,
    chunk_size: int = 50000,
    compression: str = "snappy",
    resume: bool = False,
    skip_load: bool = False,
    batch_size: int = 100000,
    commit_interval: int = 100000,
) -> None:
    """
    Main function: replicate table month-by-month with parallel workers.
    """
    # Load schema
    schema = load_schema()
    if table_name not in schema:
        print(f"[ERROR] Table {table_name} not found in schema", file=sys.stderr)
        return
    
    schema_entry = schema[table_name]
    
    # Check if table supports date filtering
    if table_name not in DATE_FILTER_COLUMNS:
        print(f"[ERROR] Table {table_name} does not support date filtering (not in DATE_FILTER_COLUMNS)", file=sys.stderr)
        return
    
    date_column = DATE_FILTER_COLUMNS[table_name]
    
    # Generate month ranges
    months = generate_month_ranges(start_date, end_date)
    print(f"[INFO] Processing {len(months)} months for {table_name}")
    print(f"[INFO] Date range: {start_date} to {end_date}")
    print()
    
    # Prepare output directory
    table_output_dir = output_dir / table_name.lower()
    table_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load checkpoint if resuming
    checkpoint = load_checkpoint(table_name, table_output_dir) if resume else {}
    completed_months = set(checkpoint.get("completed_months", []))
    failed_months = set(checkpoint.get("failed_months", []))
    
    # Determine final Parquet path
    run_suffix = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    final_parquet = table_output_dir / f"{table_name.lower()}_monthly_{run_suffix}.parquet"
    
    # If resuming and final Parquet exists, check which months are complete
    if resume and Path(checkpoint.get("final_parquet", "")).exists():
        existing_parquet = Path(checkpoint["final_parquet"])
        print(f"[INFO] Resuming from existing Parquet: {existing_parquet}")
        complete_from_parquet = get_complete_months_from_parquet(existing_parquet, date_column, months)
        completed_months.update(complete_from_parquet)
        final_parquet = existing_parquet
        print(f"[INFO] Found {len(complete_from_parquet)} complete months in existing Parquet")
        
        # Determine where process stopped
        if complete_from_parquet:
            last_complete = max(complete_from_parquet)
            # Find next month after last complete
            next_month_idx = None
            for i, (month_key, _, _) in enumerate(months):
                if month_key == last_complete:
                    if i + 1 < len(months):
                        next_month_idx = i + 1
                    break
            
            if next_month_idx is not None:
                next_month_key, next_month_start, _ = months[next_month_idx]
                print(f"[INFO] Process stopped at: {next_month_key} (incomplete month)")
                print(f"[INFO] Will only load data up to end of {last_complete} (last complete month)")
    
    # Filter months to process (skip completed)
    months_to_process = [
        (month_key, month_start, month_end)
        for month_key, month_start, month_end in months
        if month_key not in completed_months
    ]
    
    if not months_to_process:
        print(f"[INFO] All months already completed for {table_name}")
    else:
        print(f"[INFO] Processing {len(months_to_process)} remaining months")
        print(f"[INFO] Completed months: {sorted(completed_months)}")
        print()
        
        # Export months in parallel
        month_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    export_month,
                    table_name,
                    schema_entry,
                    month_key,
                    month_start,
                    month_end,
                    table_output_dir,
                    chunk_size,
                    compression,
                ): (month_key, month_start, month_end)
                for month_key, month_start, month_end in months_to_process
            }
            
            for future in as_completed(futures):
                month_key, month_start, month_end = futures[future]
                try:
                    result_month_key, rows, parquet_path = future.result()
                    if rows > 0 and parquet_path:
                        month_results.append((result_month_key, parquet_path))
                        completed_months.add(result_month_key)
                        print(f"[EXPORT] {table_name} {result_month_key}: {rows:,} rows")
                    elif rows == 0:
                        print(f"[INFO] {table_name} {month_key}: No data for this month")
                        completed_months.add(month_key)  # Mark as completed (empty month)
                    else:
                        failed_months.add(month_key)
                        print(f"[ERROR] {table_name} {month_key}: Export failed", file=sys.stderr)
                except Exception as e:
                    failed_months.add(month_key)
                    print(f"[ERROR] {table_name} {month_key}: {e}", file=sys.stderr)
        
        # Merge month Parquets into final Parquet
        if month_results:
            print(f"\n[MERGE] Merging {len(month_results)} month files into final Parquet...")
            try:
                # Check if we're appending to existing file
                append_mode = resume and final_parquet.exists()
                total_rows = merge_month_parquets(
                    table_name,
                    month_results,
                    final_parquet,
                    compression,
                    append_mode=append_mode,
                )
                print(f"[MERGE] {table_name}: Merged {total_rows:,} rows into {final_parquet}")
            except Exception as e:
                print(f"[ERROR] Merge failed: {e}", file=sys.stderr)
                return
        
        # Save checkpoint
        save_checkpoint(
            table_name,
            table_output_dir,
            sorted(completed_months),
            sorted(failed_months),
            final_parquet if final_parquet.exists() else None,
        )
    
    # Determine last complete month for loading
    # Only load data up to the last complete month (to avoid partial month data)
    if completed_months:
        # Sort months chronologically
        sorted_completed = sorted(completed_months)
        last_complete_month = sorted_completed[-1]
        
        # Find the end date of the last complete month
        last_complete_end = None
        for month_key, month_start, month_end in months:
            if month_key == last_complete_month:
                last_complete_end = month_end
                break
        
        if last_complete_end:
            load_end_date = last_complete_end
            # Convert to date for display (month_end is exclusive, so subtract 1 day for display)
            load_end_date_display = (datetime.fromisoformat(last_complete_end).date() - timedelta(days=1)).isoformat()
        else:
            load_end_date = end_date
            load_end_date_display = end_date
    else:
        load_end_date = None
        load_end_date_display = None
        last_complete_month = None
    
    # Load to SQL if not skipped
    if not skip_load and final_parquet.exists() and load_end_date:
        print(f"\n[LOAD] Loading data up to {load_end_date_display} (end of last complete month: {last_complete_month})")
        print(f"[INFO] This ensures only complete months are loaded (no partial month data)")
        try:
            with ConnectionManager() as conn_manager:
                rows_loaded = load_from_parquet_streaming(
                    table_name,
                    schema_entry,
                    final_parquet,
                    start_date,
                    load_end_date,
                    batch_size,
                    commit_interval,
                    conn_manager,
                )
                print(f"[LOAD] {table_name}: Loaded {rows_loaded:,} rows")
        except Exception as e:
            print(f"[ERROR] Load failed: {e}", file=sys.stderr)
    
    # Final summary
    print(f"\n{'='*70}")
    print(f"[SUMMARY] {table_name}")
    print(f"{'='*70}")
    print(f"  Total months: {len(months)}")
    print(f"  Completed months: {len(completed_months)}/{len(months)}")
    print(f"  Failed months: {len(failed_months)}")
    if completed_months:
        print(f"  Completed: {', '.join(sorted(completed_months))}")
    if failed_months:
        print(f"  Failed: {', '.join(sorted(failed_months))}")
    if final_parquet.exists():
        file_size_mb = final_parquet.stat().st_size / (1024 * 1024)
        print(f"  Final Parquet: {final_parquet}")
        print(f"  Parquet size: {file_size_mb:.2f} MB")
    if last_complete_month:
        print(f"  Last complete month: {last_complete_month}")
        if load_end_date_display:
            print(f"  Data loaded up to: {load_end_date_display} (end of {last_complete_month})")
    else:
        print(f"  Status: No complete months yet")
    print(f"{'='*70}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replicate table month-by-month with parallel workers and resume capability."
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
        help="Directory to store Parquet exports (default: %(default)s).",
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
        "--compression",
        choices=["snappy", "gzip", "zstd", "none", "uncompressed"],
        default="snappy",
        help="Parquet compression algorithm (default: snappy).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available.",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Export only; do not load into SQL Server target.",
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
        compression=args.compression,
        resume=args.resume,
        skip_load=args.skip_load,
        batch_size=args.batch_size,
        commit_interval=args.commit_interval,
    )


if __name__ == "__main__":
    main()

