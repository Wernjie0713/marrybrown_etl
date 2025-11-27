## Docs Index

| File | Purpose |
|------|---------|
| `PLAN.md` | High-level scope, goals, success criteria for the 1:1 replica phase. |
| `ETL.md` | Daily schedule, manual commands, table list, data-fix logic. |
| `API.md` | Strategy for rebuilding Sync Sales + subsequent endpoints. |
| `INFRA.md` | TIMEdotcom deployment details, access notes, monitoring. |
| `HISTORY.md` | Decision log so future contributors understand context. |
| `replica_schema.json` | **API development reference only** – curated list of Phase 1 tables/columns with notes for understanding relationships. Not used for replication. |
| `xilnex_full_schema.json` | **Actual schema used for replication** – full INFORMATION_SCHEMA dump (598 tables). Export script reads actual columns from this file to ensure 1:1 replication. |

Use these docs as LLM initialization material or quick refreshers before working on the project. Update them whenever the plan changes so the knowledge stays current.***
