"""
Compare daily row counts between source (Xilnex replica) and target warehouse.

Range: 2025-08-01 to 2025-10-31
Tables: Sales, Items, Payments, Credit/Debit Notes, etc.
Source schema: COM_5013
Target schema: dbo.com_5013_<TABLE>
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import pyodbc


# ---------------------------------------------------------------------------
# Config import helpers
# ---------------------------------------------------------------------------
def _import_config():
    """
    Import config.py, first trying the current repo, then the sibling marrybrown_etl.
    """
    try:
        import config  # type: ignore

        return config
    except ImportError:
        repo_root = Path(__file__).resolve().parents[1]
        sibling = repo_root.parent / "marrybrown_etl"
        if sibling.exists():
            sys.path.append(str(sibling))
            import config  # type: ignore

            return config
        raise


config = _import_config()


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 10, 31)
TABLES_TO_CHECK = {
    "APP_4_SALES": "DATETIME__SALES_DATE",
    "APP_4_SALESITEM": "DATETIME__SALES_DATE",
    "APP_4_PAYMENT": "DATETIME__DATE",
    "APP_4_VOIDSALESITEM": "DATETIME__VOID_DATETIME",
    "APP_4_SALESCREDITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESCREDITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTE": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_SALESDEBITNOTEITEM": "DATETIMEUTC_BUSINESS_DATE",
    "APP_4_EPAYMENTLOG": "TRANSACTIONDATETIME",
    "APP_4_VOUCHER": "DATETIME__VOUCHER_DATE",
}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_connection(conn_str: str) -> pyodbc.Connection:
    return pyodbc.connect(conn_str, autocommit=False)


def fetch_daily_counts(
    conn: pyodbc.Connection, table_fqn: str, date_column: str, start: date, end: date
) -> Dict[date, int]:
    """
    Return {date: count} for rows where date_column is within [start, end].
    """
    sql = f"""
        SELECT
            CAST({date_column} AS date) AS sale_date,
            COUNT(*) AS row_count
        FROM {table_fqn}
        WHERE {date_column} >= ? AND {date_column} < DATEADD(day, 1, ?)
        GROUP BY CAST({date_column} AS date)
        ORDER BY sale_date;
    """
    params = (start, end)
    results: Dict[date, int] = {}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for sale_date, row_count in cur.fetchall():
            results[sale_date] = int(row_count)
    return results


# ---------------------------------------------------------------------------
# Comparison / reporting
# ---------------------------------------------------------------------------
def compare_counts(
    source_counts: Dict[date, int], target_counts: Dict[date, int]
) -> Tuple[Dict[date, Tuple[int, int, int]], List[date]]:
    """
    Combine counts, returning per-day tuple (src, tgt, diff=tgt-src) and mismatch list.
    """
    all_dates = set(source_counts) | set(target_counts)
    comparison: Dict[date, Tuple[int, int, int]] = {}
    mismatches: List[date] = []

    for d in sorted(all_dates):
        src = source_counts.get(d, 0)
        tgt = target_counts.get(d, 0)
        diff = tgt - src  # negative means target is missing rows
        comparison[d] = (src, tgt, diff)
        if diff != 0:
            mismatches.append(d)
    return comparison, mismatches


def print_report(comparison: Dict[date, Tuple[int, int, int]], mismatches: List[date]) -> None:
    header = f"{'Date':<12}{'Source':>12}{'Target':>12}{'Diff(T-S)':>12}{'Status':>12}"
    print(header)
    print("-" * len(header))
    for d, (src, tgt, diff) in comparison.items():
        status = "OK" if diff == 0 else "MISMATCH"
        line = f"{d:%Y-%m-%d} {src:12d}{tgt:12d}{diff:12d}{status:>12}"
        print(line)

    print("\nMissing/Incomplete Dates (Diff != 0):")
    if mismatches:
        print(", ".join(d.strftime("%Y-%m-%d") for d in mismatches))
    else:
        print("None 🎉")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Force TrustServerCertificate to avoid SSL chain issues on both sides.
    source_conn_str = config.build_connection_string(
        config.AZURE_SQL_CONFIG, timeout=60, trust_server_cert=True
    )
    target_conn_str = config.build_connection_string(
        config.TARGET_SQL_CONFIG, timeout=60, trust_server_cert=True
    )

    all_mismatches: set[date] = set()

    print(f"Comparing daily counts for tables in range {START_DATE} to {END_DATE}\n")

    with get_connection(source_conn_str) as src_conn, get_connection(target_conn_str) as tgt_conn:
        for table_name, date_col in TABLES_TO_CHECK.items():
            source_table = f"[COM_5013].[{table_name}]"
            target_table = f"[dbo].[com_5013_{table_name}]"

            print("=" * 80)
            print(f"TABLE: {table_name}")
            print(f"Date column: {date_col}")
            print(f"Source: {source_table}")
            print(f"Target: {target_table}")
            print("-" * 80)

            src_counts = fetch_daily_counts(src_conn, source_table, date_col, START_DATE, END_DATE)
            tgt_counts = fetch_daily_counts(tgt_conn, target_table, date_col, START_DATE, END_DATE)

            comparison, mismatches = compare_counts(src_counts, tgt_counts)
            print_report(comparison, mismatches)
            all_mismatches.update(mismatches)
            print("\n")

    if all_mismatches:
        print("Missing Dates (Any Table):")
        print(", ".join(sorted(d.strftime("%Y-%m-%d") for d in all_mismatches)))
    else:
        print("Missing Dates (Any Table): None 🎉")


if __name__ == "__main__":
    main()
