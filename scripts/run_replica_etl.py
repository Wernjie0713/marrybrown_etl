import argparse
import subprocess
from datetime import datetime, timedelta

import pyodbc

import config


def get_target_conn():
    return pyodbc.connect(config.build_connection_string(config.TARGET_SQL_CONFIG))


def insert_run_history(run_type: str, start_date: str, end_date: str, success: bool, tables: str, message: str = None):
    conn = get_target_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO dbo.replica_run_history
        (run_type, start_timestamp, end_timestamp, start_date, end_date, processed_tables, success, error_message)
        VALUES (?, SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?, ?, ?, ?)
        """,
        run_type,
        start_date,
        end_date,
        tables,
        1 if success else 0,
        message,
    )
    conn.commit()
    conn.close()


def run_export(start_date: str, end_date: str, tables=None, full=False):
    cmd = [
        "python",
        "scripts/export_and_load_replica.py",
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]
    if tables:
        for table in tables:
            cmd.extend(["--table", table])
    if full:
        cmd.append("--full-table")
    print(f"[RUNNER] Executing: {' '.join(cmd)}")
    subprocess.check_call(cmd)


def main():
    parser = argparse.ArgumentParser(description="Run replica ETL with T-0 / T-1 workflow")
    parser.add_argument("--date", help="Reference date (YYYY-MM-DD). Defaults to yesterday.", default=None)
    parser.add_argument("--tables", action="append", help="Restrict to specific table(s).")
    parser.add_argument("--skip-t1", action="store_true", help="Skip T-1 back-check.")
    args = parser.parse_args()

    if args.date:
        base_date = datetime.fromisoformat(args.date).date()
    else:
        base_date = datetime.utcnow().date() - timedelta(days=1)

    t0_start = base_date.isoformat()
    t0_end = (base_date + timedelta(days=1)).isoformat()

    try:
        run_export(t0_start, t0_end, tables=args.tables)
        insert_run_history("T0", t0_start, t0_end, True, ",".join(args.tables or ["ALL"]))
    except subprocess.CalledProcessError as exc:
        insert_run_history("T0", t0_start, t0_end, False, ",".join(args.tables or ["ALL"]), str(exc))
        raise

    if not args.skip_t1:
        t1_date = base_date - timedelta(days=1)
        t1_start = t1_date.isoformat()
        t1_end = base_date.isoformat()
        try:
            run_export(t1_start, t1_end, tables=args.tables)
            insert_run_history("T1", t1_start, t1_end, True, ",".join(args.tables or ["ALL"]))
        except subprocess.CalledProcessError as exc:
            insert_run_history("T1", t1_start, t1_end, False, ",".join(args.tables or ["ALL"]), str(exc))
            raise


if __name__ == "__main__":
    main()


