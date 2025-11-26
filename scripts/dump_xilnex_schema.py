import json
import os
import collections

import pyodbc


CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=xilnex-mercury.database.windows.net;"
    "DATABASE=XilnexDB158;"
    "UID=BI_5013_Marrybrown;"
    "PWD=sOCJsnkH^N8m-wgMOiGd0vz%T"
)

QUERY = """
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
"""


def main():
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute(QUERY)
    rows = cursor.fetchall()

    schema = collections.OrderedDict()
    for (
        table_schema,
        table_name,
        column_name,
        data_type,
        char_len,
        num_precision,
        num_scale,
        ordinal,
    ) in rows:
        full_name = f"{table_schema}.{table_name}" if table_schema else table_name
        entry = schema.setdefault(
            full_name,
            {
                "schema": table_schema,
                "table": table_name,
                "columns": [],
            },
        )
        entry["columns"].append(
            {
                "name": column_name,
                "type": data_type,
                "char_len": char_len,
                "numeric_precision": num_precision,
                "numeric_scale": num_scale,
                "ordinal_position": ordinal,
            }
        )

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "docs",
        "xilnex_full_schema.json",
    )
    output_path = os.path.normpath(output_path)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    conn.close()
    print(f"Saved schema to {output_path}")


if __name__ == "__main__":
    main()

