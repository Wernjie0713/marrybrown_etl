## Marrybrown Data Liberation Initiative – Current Summary (Nov 2025)

- **Primary Goal**: Give Marrybrown full ownership of its sales data by replicating Xilnex POS tables into a company-controlled SQL Server warehouse, then exposing data through internal APIs and portals.

- **Current Plan (Phase 1)**  
  - 1:1 replicate the key Xilnex tables (sales, sales items, payments, products, locations, customers, promotions, staff, terminals).  
  - Limit scope to **recent data only (2024–2025)** to deliver usable analytics quickly.  
  - Schedule a daily ETL job at **2 AM** that:  
    1. Loads yesterday's transactions (T-0).  
    2. Re-checks the previous day (T-1) for late fixes or voids and syncs our warehouse accordingly.  
  - Provide a manual command to re-run ETL for a specific date/range when needed.  
  - Add indexes/optimizations **after** replication; schema stays identical to Xilnex for now.
  - Artifact references:
    - `docs/replica_schema.json` → **API development reference only** (curated columns with relationship notes).
    - `docs/xilnex_full_schema.json` → **Actual schema used for replication** (full 598-table dump; export script reads from this).
    - `scripts/generate_migration_from_schema.py` → regenerate migration SQL from actual Xilnex schema.
    - `scripts/dump_xilnex_schema.py` / `scripts/count_xilnex_tables.py` → regenerate schema or verify counts.

- **API Strategy**  
  - First priority: replicate Xilnex's **Sync Sales API** from our warehouse (serves as the template).  
  - Reverse-engineer required joins by comparing Xilnex responses with our raw tables.  
  - Once Sync Sales matches, extend to Daily Sales / EOD Summary / Product Mix endpoints.  
  - Document discovered join logic because Xilnex docs only list parameters, not relationships.

- **Infrastructure Notes**  
  - SQL Server 2022 + FastAPI + React portal already deployed on TIMEdotcom cloud.  
  - Xilnex restricts DB access by IP; ensure ETL server IP (or VPN endpoint) stays whitelisted.  
  - SSL certificate trust auto-detected for local connections (localhost uses `TrustServerCertificate=yes`).  
  - Legacy optimized Star-Schema assets, chunked ETL scripts, and related docs now live under `archive/legacy_api_pipeline/` for reference.
  - Export script includes `--skip-existing` flag to avoid re-processing already-loaded tables.

- **Future Recommendation (Outside current scope)**  
  - Revisit a proper Star Schema once the business has stable ownership of the replicated data.  
  - Backfill older history (2018–2023) when bandwidth allows.  
  - Introduce data-type normalization (DATETIME, DECIMAL) and covering indexes as part of the next phase.
