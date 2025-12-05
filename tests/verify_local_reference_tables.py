"""
Verify row counts for reference tables replicated to the local warehouse.

Reference tables are derived from the replica schema minus DATE_FILTER_COLUMNS
as defined in scripts/replicate_reference_tables.py.
"""

import sys
from pathlib import Path

import pyodbc

# Add project root to import replicate_reference_tables utilities
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.replicate_reference_tables import DATE_FILTER_COLUMNS, load_schema  # noqa: E402

# Local warehouse connection credentials
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=MarryBrown_DW;"
    "UID=etl_user;"
    "PWD=YourSecurePassword123!;"
    "TrustServerCertificate=yes;"
)

SCHEMA_PREFIX = "dbo.com_5013_"


def get_reference_tables():
    """Return sorted list of tables that are not date-filtered (reference tables)."""
    schema = load_schema()
    return sorted(table for table in schema.keys() if table not in DATE_FILTER_COLUMNS)


def check_table_counts():
    tables = get_reference_tables()

    print("=" * 60)
    print("Local Warehouse Reference Data Verification")
    print("=" * 60)
    print("Server: localhost")
    print("Database: MarryBrown_DW")
    print("=" * 60)

    try:
        conn = pyodbc.connect(CONN_STR, timeout=30)
    except Exception as exc:
        print(f"\nConnection failed: {exc}")
        return

    try:
        cursor = conn.cursor()
        print("\nConnected to local warehouse.\n")

        print(f"{'Table Name':<40} {'Row Count':>15}")
        print("-" * 57)

        total_rows = 0
        for table in tables:
            full_table_name = f"{SCHEMA_PREFIX}{table}"
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                count = cursor.fetchone()[0]
                total_rows += count
                print(f"{table:<40} {count:>15,}")
            except Exception as exc:
                print(f"{table:<40} ERROR: {exc}")

        print("-" * 57)
        print(f"{'TOTAL':>40} {total_rows:>15,}")

    finally:
        conn.close()


if __name__ == "__main__":
    check_table_counts()
