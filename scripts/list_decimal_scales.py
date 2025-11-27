"""
Utility script to show decimal columns whose precision/scale differ from 38,20.
"""
import sys
from pathlib import Path

import pyodbc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

TABLES = [
    "APP_4_ITEM",
    "APP_4_STOCK",
    "APP_4_CUSTOMER",
    "APP_4_POINTRECORD",
    "LOCATION_DETAIL",
    "APP_4_VOUCHER_MASTER",
    "APP_4_CASHIER_DRAWER",
    "APP_4_EXTENDEDSALESITEM",
]


def main():
    conn = pyodbc.connect(
        config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    )
    cursor = conn.cursor()

    query = """
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?
          AND DATA_TYPE IN ('decimal', 'numeric')
          AND (NUMERIC_PRECISION <> 38 OR NUMERIC_SCALE <> 20)
    """

    for table in TABLES:
        cursor.execute(query, (f"com_5013_{table}",))
        rows = cursor.fetchall()
        if rows:
            print(f"{table} has non-(38,20) decimals:")
            for row in rows:
                print(
                    f"  {row.COLUMN_NAME}: {row.DATA_TYPE} "
                    f"(precision={row.NUMERIC_PRECISION}, scale={row.NUMERIC_SCALE})"
                )
        else:
            print(f"{table}: all decimals are (38,20)")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

