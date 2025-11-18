# Documentation Index — Marrybrown ETL

Concise guide to the docs that remain active after the November 2025 cleanup.

---

## 🚀 Getting Started

| Document | Purpose |
|----------|---------|
| **[README.md](README.md)** | High-level overview plus architecture summary |
| **[docs/QUICKSTART.md](docs/QUICKSTART.md)** | 10-minute local bootstrap |
| **[QUICK_START_GUIDE.md](QUICK_START_GUIDE.md)** | Detailed checklist for first-time operators |
| **[QUICKSTART_LOCAL_TEST.md](QUICKSTART_LOCAL_TEST.md)** | Sanity test instructions for on-prem SQL Server |
| **[QUICKSTART_API_TEST.md](QUICKSTART_API_TEST.md)** | Steps for exercising the Xilnex API sandbox |

---

## 📚 Core References

### API ETL Flow
- `CHUNKED_ETL_GUIDE.md` – end-to-end walkthrough of the chunked pipeline
- `CHUNKED_APPROACH_SUMMARY.md` – executive summary for stakeholders
- `TESTING_API_ETL.md` – manual and automated verification steps
- `GAMMA_AI_PROMPT.md` – canned prompt for regenerating SQL via Gamma

### Connectivity & Operations
- `DATABASE_CONNECTION_FIX.md` – SQL Server auth fixes and troubleshooting
- `TROUBLESHOOTING_STUCK_ETL.md` – runbook for recovering stalled jobs
- `ETL_BUG_PREVENTION_GUIDE.md` – guardrails for new feature work (archived copy lives in `archive/docs`)

### Data Model
- `docs/DATABASE_SCHEMA.md` – canonical star schema
- `docs/PROJECT_CONTEXT.md` – business context and reporting requirements

---

## 🗂️ Repository Structure (Nov 2025)

```
marrybrown_etl/
├── api_etl/             # API-first pipeline (extract/transform/load)
├── direct_db_etl/       # Legacy warehouse loaders (dim/fact scripts)
├── scripts/             # Operational utilities (migrations, health checks)
├── tests/               # Standalone connection/API tests
├── migrations/          # 001-050 numbered migrations (new canonical set)
├── archive/
│   ├── sql/             # Superseded schema hotfixes
│   └── docs/            # Historical write-ups & presentations
├── debug/               # Investigation scripts and comparison artifacts
└── docs/                # Quickstart + schema references
```

Use `scripts/run_migration.py` to apply the numbered migrations in order (001 → 050).

---

## 📖 Role-Based Path

- **Pipeline Operators**: `README.md` → `QUICK_START_GUIDE.md` → `CHUNKED_ETL_GUIDE.md`
- **Data Engineers**: `docs/DATABASE_SCHEMA.md` → `CHUNKED_APPROACH_SUMMARY.md` → `TESTING_API_ETL.md`
- **Support / SRE**: `DATABASE_CONNECTION_FIX.md` → `TROUBLESHOOTING_STUCK_ETL.md`

---

## 🧭 Where Did Everything Else Go?

- Legacy SQL fixes & investigations → `archive/sql/`
- Presentation decks, comparison studies, historical incident reports → `archive/docs/`
- Direct database ETL scripts → `direct_db_etl/`

This keeps the root clean while preserving history when needed.

---

## 🛠 Maintenance Notes

- Update this index whenever documentation moves or new directories are introduced.
- Keep `archive/` read-only unless a file is definitively obsolete.
- When adding a new runbook, reference it here and in `README.md`.

---

**Maintainer**: Yong Wern Jie (MIS Department)  
**Last Updated**: November 18, 2025  
**Documentation Version**: 2.0.0

