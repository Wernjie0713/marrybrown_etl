# file: scripts/count_xilnex_tables.py
import pyodbc

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=xilnex-mercury.database.windows.net;"
    "DATABASE=XilnexDB158;"
    "UID=BI_5013_Marrybrown;"
    "PWD=sOCJsnkH^N8m-wgMOiGd0vz%T"
)

def main():
    conn = pyodbc.connect(CONN_STR)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
    total = cur.fetchone()[0]
    conn.close()
    print(f"Total tables: {total}")

if __name__ == "__main__":
    main()