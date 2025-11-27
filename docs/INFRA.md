## Infrastructure Notes – TIMEdotcom Deployment

### Servers
| Role | Hostname | Specs | Network |
|------|----------|-------|---------|
| **API + Portal** | `marrybrown-web-01-obsxb` (AlmaLinux 10) | 4 vCPU / 8 GB RAM / 80 GB + 200 GB disks | 10.0.0.15 (DMZ) / 211.25.163.147 |
| **Database** | `marrybrown-db-01-obsxb` (Windows Server 2025) | 8 vCPU / 16 GB RAM / 150 GB + 300 GB disks | 10.0.1.194 (LAN) / 211.25.163.117 |

### Software
- SQL Server 2022 (Developer) – `MarryBrown_DW`
- FastAPI backend (Podman container)
- React portal (Vite + Nginx container)
- VPN: OpenVPN profile required before accessing internal IPs

### Access Notes
- **SQL Auth**: `sa` / `NewSA@Password2025!` (admin) – `etl_user` / `ETL@MarryBrown2025!` (scripts)
- **VPN**: `marrybrown` / `+r=p44etrUt1` (primary), backups available
- **IP Whitelisting**: Xilnex restricts direct DB access; keep ETL server IP registered. Company WiFi changes often—prefer static IP or always run ETL from cloud server.

### Directories (current repo)
- `archive/legacy_api_pipeline/` – previous star-schema + API-based ETL assets
- `docs/` – active documentation for 1:1 replica plan (schemas, infra, history)
  - `replica_schema.json` – **API development reference only** (curated columns with notes)
  - `xilnex_full_schema.json` – **Actual schema used for replication** (full 598-table dump)
- `exports/` – Parquet dumps produced by `scripts/export_and_load_replica.py` (can be cleaned between runs)
- `migrations/schema_tables/` – SQL migration files
  - `000_drop_all_tables.sql` – Drop all replica tables (use before recreating)
  - `100_create_replica_tables.sql` – Auto-generated from actual Xilnex schema
  - `110_create_replica_metadata_tables.sql` – ETL tracking tables
- `scripts/` – ETL automation
  - `generate_migration_from_schema.py` – Regenerate migration SQL from Xilnex schema
  - `export_and_load_replica.py` – Main export/load script (uses actual schema)
  - `run_replica_etl.py` – T-0/T-1 orchestration

### Monitoring / Ops
- Windows Task Scheduler (planned) for 2 AM ETL trigger
- `etl_replica_progress` + `replica_run_history` tables capture per-table metrics and run summaries
- `scripts/run_replica_etl.py` handles T‑0/T‑1 orchestration; use `.env.local` for local dry runs

