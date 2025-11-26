## Decision Log

| Date | Decision | Notes |
|------|----------|-------|
| Oct 2025 | Built optimized Star Schema + API-based ETL | Chunked extractor with checkpointing, fast sample mode, portal validated vs Xilnex DB |
| Nov 2025 | Switched to **1:1 replica** plan | API limitations (sequential, 1000-limit, no date range) made historical extraction impractical; business needs data ownership ASAP |
| Nov 2025 | Archived legacy assets | `api_etl/`, direct DB loaders, migrations, docs moved to `archive/legacy_api_pipeline/` for reference |
| Nov 2025 | Created `/docs` knowledge base | CLAUDE summary + Markdown briefs (Plan, ETL, API, Infra, History) to keep LLM context current |

### Outstanding Ideas (Future Phase Suggestions)
- Reintroduce star schema once replica proves value.
- Backfill 2018–2023 history when bandwidth allows.
- Add data-type normalization + indexing after replication stabilizes.

