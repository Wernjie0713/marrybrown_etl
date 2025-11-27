"""
Check for string columns whose values exceed the target column length.
Usage: python scripts/find_string_overflows.py APP_4_CUSTOMER
"""
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.export_and_load_replica import load_schema  # noqa: E402


def main(table_name: str):
    schema = load_schema().get(table_name)
    if not schema:
        raise SystemExit(f"Schema for {table_name} not found")

    columns = [
        col
        for col in schema["columns"]
        if col["type"].upper() in {"VARCHAR", "NVARCHAR", "CHAR", "NCHAR"}
        and col.get("char_len") not in (None, -1)
    ]

    export_dir = PROJECT_ROOT / "exports" / table_name.lower()
    parquet_files = sorted(export_dir.glob("*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No parquet files in {export_dir}")
    parquet_path = parquet_files[-1]
    print(f"Inspecting {parquet_path}")

    df = pd.read_parquet(parquet_path)
    overflows = []

    for col in columns:
        name = col["name"]
        limit = col.get("char_len") or 0
        if name not in df.columns or limit <= 0:
            continue
        lengths = df[name].astype(str).str.len()
        max_len = lengths.max()
        if max_len > limit:
            sample = df.loc[lengths.idxmax(), name]
            overflows.append((name, limit, int(max_len), sample[:100]))

    if not overflows:
        print("No string columns exceed their defined length.")
        return

    print("Columns exceeding defined length:")
    for name, limit, max_len, sample in overflows:
        print(f"- {name}: limit {limit}, max {max_len}, sample='{sample}'")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/find_string_overflows.py <TABLE>")
    main(sys.argv[1])

