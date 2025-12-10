"""Quick check for missing dates in APP_4_SALES table"""
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config
import pyodbc

START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 10, 31)

source_conn_str = config.build_connection_string(config.AZURE_SQL_CONFIG, trust_server_cert=True)
target_conn_str = config.build_connection_string(config.TARGET_SQL_CONFIG, trust_server_cert=True)

print(f"Checking APP_4_SALES from {START_DATE} to {END_DATE}...")

sql = """
    SELECT 
        YEAR(DATETIME__SALES_DATE) as yr,
        MONTH(DATETIME__SALES_DATE) as mo,
        COUNT(*) as cnt
    FROM {table}
    WHERE DATETIME__SALES_DATE >= ? AND DATETIME__SALES_DATE <= ?
    GROUP BY YEAR(DATETIME__SALES_DATE), MONTH(DATETIME__SALES_DATE)
    ORDER BY yr, mo
"""

with pyodbc.connect(source_conn_str) as src_conn:
    src_cursor = src_conn.cursor()
    src_cursor.execute(sql.format(table='[COM_5013].[APP_4_SALES]'), (START_DATE, END_DATE))
    src_counts = {(r.yr, r.mo): r.cnt for r in src_cursor.fetchall()}

with pyodbc.connect(target_conn_str) as tgt_conn:
    tgt_cursor = tgt_conn.cursor()
    tgt_cursor.execute(sql.format(table='[dbo].[com_5013_APP_4_SALES]'), (START_DATE, END_DATE))
    tgt_counts = {(r.yr, r.mo): r.cnt for r in tgt_cursor.fetchall()}

print("\n" + "="*70)
print(f"{'Month':<12}{'Source':>15}{'Target':>15}{'Diff':>12}{'Status':>12}")
print("-"*70)

months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct']
for mo_num in range(1, 11):
    key = (2025, mo_num)
    src = src_counts.get(key, 0)
    tgt = tgt_counts.get(key, 0)
    diff = tgt - src
    status = "OK" if diff == 0 else "MISSING" if diff < 0 else "EXTRA"
    print(f"{months[mo_num-1]} 2025{src:>12,}{tgt:>15,}{diff:>12,}{status:>12}")

print("-"*70)
total_src = sum(src_counts.values())
total_tgt = sum(tgt_counts.values())
print(f"{'TOTAL':<12}{total_src:>15,}{total_tgt:>15,}{total_tgt-total_src:>12,}")
