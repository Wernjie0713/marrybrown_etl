# Work Log - 5th December

## Summary

Started the API development phase by updating the **Location API** to use the new 1:1 Xilnex Replica schema. This replaces the outdated Star Schema approach and ensures the API queries the correct tables and columns from the replicated data.

---

## 1. Location API Update

### Objective

Update `routers/locations.py` to query the `dbo.com_5013_LOCATION_DETAIL` table instead of the legacy `dim_locations` table.

### Changes Made

- **Source Table:** Switched from `dim_locations` to `dbo.com_5013_LOCATION_DETAIL`.
- **Column Mapping:**
  - `LOCATION_INT_ID` → `location_key` (Integer)
  - `LOCATIONNAME` → `name`
  - `LOCATIONADDRESS` → `address`
- **Filtering:** Added `WHERE LOCATION_DELETED = 0` to ensure only active locations are returned.
- **Ordering:** Ordered results by `LOCATIONNAME`.

### Result

The `/locations` endpoint now correctly returns active store locations from the replicated Xilnex data.

---

## 2. Xilnex Replica Connection Support

### Issue

Xilnex support reported that our ETL was querying the **primary (production) database**, impacting live POS operations. They advised adding `ApplicationIntent=ReadOnly` to the connection string to route queries to the **read-only replica** instead.

### Solution

Updated `config.py` to support an optional `XILNEX_APPLICATION_INTENT` environment variable:

- **New env var:** `XILNEX_APPLICATION_INTENT=ReadOnly`
- **Behavior:** When set, appends `ApplicationIntent=ReadOnly;` to the ODBC connection string.
- **Effect:** SQL Server routes the connection to the secondary (read-only) replica, offloading ETL reads from the primary database.

### How Replica Works

- **Sync:** Near real-time (milliseconds to seconds delay via asynchronous replication).
- **Impact:** Negligible for daily/monthly ETL runs—effectively "live" data.

### Files Changed

- `config.py`: Added `application_intent` to `AZURE_SQL_CONFIG` and updated `build_connection_string()`.

### To Enable (on VM)

Add to `.env`:

```ini
XILNEX_APPLICATION_INTENT=ReadOnly
```
