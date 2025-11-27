## Decision Log

| Date | Decision | Notes |
|------|----------|-------|
| Oct 2025 | Built optimized Star Schema + API-based ETL | Chunked extractor with checkpointing, fast sample mode, portal validated vs Xilnex DB |
| Nov 2025 | Switched to **1:1 replica** plan | API limitations (sequential, 1000-limit, no date range) made historical extraction impractical; business needs data ownership ASAP |
| Nov 2025 | Archived legacy assets | `api_etl/`, direct DB loaders, migrations, docs moved to `archive/legacy_api_pipeline/` for reference |
| Nov 2025 | Created `/docs` knowledge base | CLAUDE summary + Markdown briefs (Plan, ETL, API, Infra, History) to keep LLM context current |
| Nov 2025 | Bootstrapped replica ETL tooling | Added replica migrations + new scripts (`export_and_load_replica.py`, `run_replica_etl.py`) leveraging bulk-export + Parquet load workflow |
| Nov 2025 | Fixed schema replication approach | Updated to use actual columns from `xilnex_full_schema.json` instead of curated `replica_schema.json`; created `generate_migration_from_schema.py` to auto-generate migrations; fixed SSL certificate issues for local connections; added `--skip-existing` flag to avoid re-processing loaded tables |

### Outstanding Ideas (Future Phase Suggestions)
- Reintroduce star schema once replica proves value.
- Backfill 2018–2023 history when bandwidth allows.
- Add data-type normalization + indexing after replication stabilizes.

