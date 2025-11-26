## Project Plan – Marrybrown Data Liberation (Nov 2025)

### Mission
Replicate key Xilnex POS tables (sales, sales items, payments, products, locations, customers, promotions, staff, terminals) into our own SQL Server warehouse so Marrybrown fully controls its sales data and can power internal APIs + React portal without depending on Xilnex.

### Scope (Phase 1)
- **Data window**: 2024–2025 only (recent, actionable data).
- **Schema**: 1:1 replica of Xilnex tables; optimization deferred.
- **ETL cadence**: Automated daily run at 2 AM + manual trigger option.
- **APIs**: Re-implement Sync Sales first, then Daily Sales, EOD Summary, Product Mix.
- **Portal**: Existing React dashboard continues, but now points to our warehouse.

### Out-of-Scope / Future Phases
- Historical backfill (2018–2023).
- Star schema redesign with DATETIME/DECIMAL normalization.
- Advanced indexing, materialized views, or cube layer.

### Success Criteria
1. Warehouse mirrors Xilnex tables for 2024–2025 with <24h latency.
2. Sync Sales API response produced from our warehouse matches Xilnex reference.
3. Daily ETL supports reruns + T-1 data fix reconciliation.
4. Documentation + handover instructions kept current in `/docs`.

