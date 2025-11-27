## API Strategy – Replica-Based Implementation

### Goal
Rebuild critical Xilnex APIs on top of our warehouse so internal systems consume Marrybrown-owned data. Start with high-value endpoints that power the React portal.

### Priority Order
1. **Sync Sales API (replica)**
2. **Daily Sales Report**
3. **EOD Sales Summary**
4. **Product Mix / Item-level Sales**

### Approach
1. **Use Xilnex Sync Sales response as reference.**  
   - Inspect JSON payload (sales header, items, payments, outlet info).  
   - Map each field to corresponding Xilnex tables (`APP_4_SALES`, `APP_4_SALESITEM`, etc.).  
   - **Reference:** `docs/replica_schema.json` contains curated column notes for API development (but replication uses actual schema from `xilnex_full_schema.json`).
2. **Write SQL JOIN blueprint.**  
   - `SALES_NO` ties sales ↔ items ↔ payments.  
   - `SALE_LOCATION` (`GUID`) joins to `LOCATION_DETAIL`.  
   - `ITEM_CODE` maps via `ATLTERNATE_LOOK_UP` in `APP_4_ITEM`.  
3. **Implement FastAPI endpoint** that executes the JOINs, formats response, and handles pagination/date filters (even if source schema lacks them).  
4. **Validate** by comparing sample outputs to Xilnex Sync Sales API.

### Known Join Relationships
```
APP_4_SALES.SALES_NO          = APP_4_SALESITEM.SALES_NO
APP_4_SALES.SALES_NO          = APP_4_PAYMENT.INVOICE_ID
APP_4_SALES.SALE_LOCATION     = LOCATION_DETAIL.ID
APP_4_SALESITEM.ITEM_CODE     = APP_4_ITEM.ATLTERNATE_LOOK_UP
APP_4_SALES.CUSTOMER_ID       = APP_4_CUSTOMER.ID
```

### Response Design Guidelines
- Keep structure compatible with existing frontend (arrays of items, payments).
- Normalize dates/times before returning (ISO 8601).
- Include pagination metadata (`nextTimestamp` or date cursor) for long result sets.
- Document every joined column; Xilnex docs list parameters only, not relationships.

### Future Enhancements
- Create API contracts in `/docs/api/` once schemas stabilize.
- Introduce caching layer once warehouse load is understood.
- Add feature flags to switch between replica + future optimized schema.

