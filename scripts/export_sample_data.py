"""
Export sample rows from replicated sales and reference tables to JSON files.

Usage:
    python scripts/export_sample_data.py --sample-size 10 --output-dir exports/sample_data
"""

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, List, Sequence

import pyodbc

# Make project modules importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.replicate_reference_tables import DATE_FILTER_COLUMNS, load_schema  # noqa: E402

# Cloud warehouse (sales tables)
CLOUD_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=10.0.1.194,1433;"
    "DATABASE=MarryBrown_DW;"
    "UID=etl_user;"
    "PWD=ETL@MarryBrown2025!;"
    "TrustServerCertificate=yes;"
)

# Local warehouse (reference tables)
LOCAL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=MarryBrown_DW;"
    "UID=etl_user;"
    "PWD=YourSecurePassword123!;"
    "TrustServerCertificate=yes;"
)

SCHEMA_PREFIX = "dbo.com_5013_"

# Sales tables (from verify_replication.py)
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


def get_reference_tables() -> List[str]:
    """Tables not in DATE_FILTER_COLUMNS are treated as reference tables."""
    schema = load_schema()
    return sorted(table for table in schema.keys() if table not in DATE_FILTER_COLUMNS)


def to_jsonable(value):
    """Convert database values to JSON-serializable primitives."""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, memoryview, bytearray)):
        return bytes(value).hex()
    return value


def fetch_sample_rows(
    conn_str: str,
    tables: Iterable[str],
    sample_size: int,
) -> List[tuple[str, List[dict]]]:
    """Fetch up to sample_size rows for each table."""
    results = []
    conn = pyodbc.connect(conn_str, timeout=30)
    cursor = conn.cursor()
    for table in tables:
        full_name = f"{SCHEMA_PREFIX}{table}"
        try:
            cursor.execute(f"SELECT TOP {sample_size} * FROM {full_name}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            table_data = [
                {col: to_jsonable(val) for col, val in zip(columns, row)}
                for row in rows
            ]
            results.append((table, table_data))
        except Exception as exc:
            results.append((table, [{"error": str(exc)}]))
    conn.close()
    return results


def write_samples(
    samples: Sequence[tuple[str, List[dict]]],
    output_dir: Path,
):
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, rows in samples:
        out_path = output_dir / f"{table.lower()}.json"
        out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        print(f"[OK] {table}: wrote {len(rows)} rows to {out_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export sample rows from sales and reference tables to JSON."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of rows to fetch per table (default: 10).",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("exports") / "sample_data",
        help="Base output directory for JSON files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    base_output = Path(args.output_dir)

    print("Exporting sales table samples from cloud warehouse...")
    sales_samples = fetch_sample_rows(CLOUD_CONN_STR, SALES_TABLES, args.sample_size)
    write_samples(sales_samples, base_output / "sales")

    print("\nExporting reference table samples from local warehouse...")
    reference_tables = get_reference_tables()
    ref_samples = fetch_sample_rows(LOCAL_CONN_STR, reference_tables, args.sample_size)
    write_samples(ref_samples, base_output / "reference")

    print("\nDone.")


if __name__ == "__main__":
    main()
