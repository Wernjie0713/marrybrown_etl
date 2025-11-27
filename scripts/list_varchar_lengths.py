"""List string column lengths for APP_4_CUSTOMER."""
from pathlib import Path
import sys

import pyodbc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402


def main():
    conn = pyodbc.connect(
        config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)
    )
    cursor = conn.cursor()

    query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = 'com_5013_APP_4_CUSTOMER'
          AND DATA_TYPE IN ('varchar','nvarchar','char','nchar')
        ORDER BY CHARACTER_MAXIMUM_LENGTH
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        print(
            f"{row.COLUMN_NAME:40s} {row.DATA_TYPE:8s} len={row.CHARACTER_MAXIMUM_LENGTH}"
        )

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

