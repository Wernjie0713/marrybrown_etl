"""
Orchestrate sequential replication of all sales tables using the monthly streaming pipeline.

Usage:
    python scripts/replicate_all_sales_data.py --start-date 2025-10-01 --end-date 2025-11-30 --max-workers 2
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path so we can import peer modules
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.replicate_monthly_parallel_streaming import replicate_monthly_parallel  # noqa: E402
import config  # noqa: E402


# Tables to process (must be present in DATE_FILTER_COLUMNS in replicate_reference_tables.py)
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replicate all sales-related tables sequentially using monthly streaming."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (inclusive) in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (exclusive) in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Parallel workers per table (default: %(default)s).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Row chunk size for streaming exports (default matches replicate_monthly_parallel).",
    )
    parser.add_argument(
        "--commit-interval",
        type=int,
        default=100000,
        help="Rows per commit interval (default matches replicate_monthly_parallel).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available for each table.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(config.EXPORT_DIR)

    for table in SALES_TABLES:
        print(f"\n{'='*70}")
        print(f"[RUN] Replicating {table}")
        print(f"{'='*70}")
        replicate_monthly_parallel(
            table_name=table,
            start_date=args.start_date,
            end_date=args.end_date,
            output_dir=output_dir,
            max_workers=args.max_workers,
            chunk_size=args.chunk_size,
            resume=args.resume,
            commit_interval=args.commit_interval,
        )


if __name__ == "__main__":
    main()
