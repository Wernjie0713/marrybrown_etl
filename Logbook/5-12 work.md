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

### Verification ✅

Ran `python tests/test_connections.py` on VM with `XILNEX_APPLICATION_INTENT=ReadOnly`:

```
✅ Successfully connected to Xilnex source database!
   Server: xilnex-mercury.database.windows.net
   Database: XilnexDB158
   ApplicationIntent: ReadOnly
   Connection Type: 🔄 REPLICA (Read-Only)
```

**Result:** ETL is now connected to the read-only replica. Future replication runs will not impact the primary POS database.

---

## 3. Complete Warehouse Verification

Ran reference table replication from VM via replica connection, then verified all tables:

```
python scripts/replicate_reference_tables.py --full-table
python tests/verify_replication.py
```

### Current Data in Cloud Warehouse

| Category        | Table                        |           Rows |
| --------------- | ---------------------------- | -------------: |
| **Sales**       | APP_4_SALES                  |      2,853,438 |
|                 | APP_4_SALESITEM              |      7,900,000 |
|                 | APP_4_PAYMENT                |         44,125 |
|                 | APP_4_VOIDSALESITEM          |          2,912 |
|                 | APP_4_VOUCHER                |         73,670 |
|                 | _Others (CN/DN/EPayment)_    |              0 |
| **Subtotal**    |                              | **10,874,145** |
| **Reference**   | APP_4_POINTRECORD            |      3,734,108 |
|                 | APP_4_CUSTOMER               |        905,321 |
|                 | APP_4_STOCK                  |        175,297 |
|                 | APP_4_ITEM                   |         10,304 |
|                 | APP_4_CASHIER_DRAWER         |          3,102 |
|                 | APP_4_VOUCHER_MASTER         |          1,806 |
|                 | LOCATION_DETAIL              |            315 |
|                 | _Others (Delivery/Extended)_ |              0 |
| **Subtotal**    |                              |  **4,830,253** |
| **GRAND TOTAL** |                              | **15,704,398** |

### Scripts Updated

- `tests/verify_replication.py`: Now checks ALL 19 tables (sales + reference) with subtotals.
- `tests/verify_local_reference_tables.py`: Updated to use `config.py` for connection.
- `config.py`: Default target changed to cloud server `10.0.1.194,1433`.
