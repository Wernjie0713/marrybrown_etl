from pathlib import Path
import sys

import pyodbc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402


COLUMNS_TO_CHECK = [
    ("APP_4_POINTRECORD", "DOCUMENT_ID"),
    ("APP_4_ITEM", "PAIR_ID"),
    ("APP_4_VOUCHER_MASTER", "CAMPAIGN_TARGET"),
    ("APP_4_VOUCHER_MASTER", "MAX_REDEMPTION_PER_CLIENT"),
    ("APP_4_VOUCHER_MASTER", "MAX_REDEMPTION_PER_CAMPAIGN"),
    ("APP_4_ITEM", "DOUBLE_COST"),
    ("APP_4_STOCK", "INT_AVAILABLE_QUANTITY"),
    ("APP_4_STOCK", "INT_ONHAND_QUANTITY"),
    ("APP_4_STOCK", "INT_EXTEND_2"),
    ("APP_4_CASHIER_DRAWER", "INT_EXTEND_1"),
    ("APP_4_CASHIER_DRAWER", "INT_EXTEND_2"),
]


def main():
    conn = pyodbc.connect(
        config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    )
    cursor = conn.cursor()

    query = """
        SELECT
            DATA_TYPE,
            ISNULL(NUMERIC_PRECISION, 0) AS numeric_precision,
            ISNULL(NUMERIC_SCALE, 0) AS numeric_scale,
            ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS char_length
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?
          AND COLUMN_NAME = ?
    """

    print("Target schema column check:\n")
    for table, column in COLUMNS_TO_CHECK:
        cursor.execute(query, (f"com_5013_{table}", column))
        row = cursor.fetchone()
        if row:
            print(
                f"{table}.{column}: {row.DATA_TYPE.upper()} "
                f"(precision={row.numeric_precision}, scale={row.numeric_scale}, "
                f"char_len={row.char_length})"
            )
        else:
            print(f"{table}.{column}: NOT FOUND")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

