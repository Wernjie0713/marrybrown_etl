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
- `docs/` – active documentation for 1:1 replica plan
- `data validation/` – JSON exports + comparison scripts

### Monitoring / Ops
- Windows Task Scheduler (planned) for 2 AM ETL trigger
- `etl_progress` + planned run logs capture success/failure + record counts
- Manual rerun script to be placed under `scripts/` once new pipeline is coded

