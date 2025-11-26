## Debug & Investigation Utilities — Marrybrown ETL

This folder contains **one-off analysis, comparison, and exploration tools** that were used to validate the ETL and reconcile differences between Xilnex reports, the API, and the warehouse.

- **Python scripts (root of this folder)**  
  Investigation and comparison helpers such as:
  - `investigate_sales_calculation.py`
  - `explore_xilnex_api.py`
  - `compare_api_vs_warehouse.py`
  - other `compare_*` and `explore_*` scripts

- **Data artifacts (`data/`)**  
  Raw API responses and comparison outputs:
  - Large `xilnex_sales_response_*.json` snapshots
  - Excel workbooks like `*_Comparison_*.xlsx`
  - Text summaries such as `product_mix_comparison_report.txt`

These are **not required for the standard ETL run** (which lives under `api_etl/`, `direct_db_etl/`, `scripts/`, and `migrations/`), but are kept here as a **toolbox for deep dives, incident analysis, and regression checks**.

When adding new investigation scripts or one-off comparison outputs, place them here instead of the repository root.


