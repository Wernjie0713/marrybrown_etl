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

---

## 4. Data Quality Verification Script

Created `tests/verify_data_quality.py` to compare data between **Source (Xilnex replica)** and **Target (Cloud warehouse)**.

### Checks Performed

1. **Row Count Comparison** – Source vs Target row counts for all 19 tables
2. **Random Sample Comparison** – 5 random rows per table, column-by-column comparison

### Initial Results (5 Dec 2025, 2:12 PM)

**Reference Tables:**
| Table | Source | Target | Status |
|-------|--------|--------|--------|
| APP_4_ITEM | 10,324 | 10,324 | ✅ MATCH |
| LOCATION_DETAIL | 319 | 319 | ✅ MATCH |
| APP_4_VOUCHER_MASTER | 1,814 | 1,814 | ✅ MATCH |
| APP_4_STOCK | 175,944 | 175,901 | ⚠️ -43 (live data) |
| APP_4_CUSTOMER | 908,812 | 908,667 | ⚠️ -145 (live data) |
| APP_4_POINTRECORD | 3,811,857 | 3,809,492 | ⚠️ -2,365 (live data) |

**Sales Tables (need more data):**
| Table | Source (≥Oct 1) | Target | Gap |
|-------|-----------------|--------|-----|
| APP_4_SALES | 3,108,249 | 44,579 | -3M |
| APP_4_SALESITEM | 16,228,095 | 227,418 | -16M |
| APP_4_PAYMENT | 3,081,468 | 44,125 | -3M |

### Notes

- Reference table differences are **expected** (live production data changes constantly)
- Sales tables have large gaps because only partial replication was done
- **Next Step:** Replicating Aug–Oct 2025 data, will verify again after completion

---

## 5. API Users Table Migration

Created `migrations/schema_tables/200_create_users_table.sql` for API authentication.

### Table: `dbo.api_users`

| Column          | Type          | Notes             |
| --------------- | ------------- | ----------------- |
| id              | INT IDENTITY  | Primary key       |
| email           | NVARCHAR(255) | Unique, not null  |
| hashed_password | NVARCHAR(255) | bcrypt hash       |
| is_active       | BIT           | Default 1         |
| is_superuser    | BIT           | Default 0         |
| created_at      | DATETIME      | Default GETDATE() |
| updated_at      | DATETIME      | Default GETDATE() |

### Sample User

- Email: `user@example.com`
- Password: `password` (bcrypt cost 12)

### ✅ API Code Updated

Updated API authentication to use `dbo.api_users`:

- `routers/auth.py`: All queries now point to `dbo.api_users`; removed `full_name` field
- `security.py`: `User` model trimmed; `get_user_by_id` selects from `dbo.api_users`
- `.env`: Updated with cloud DB connection (`10.0.1.194,1433`)

### ✅ API Testing Verified (5 Dec 2025, 3:00 PM)

Tested via Swagger UI at `http://localhost:8100/docs`:

| Endpoint              | Status     | Notes                        |
| --------------------- | ---------- | ---------------------------- |
| POST /api/auth/signin | ✅ Working | JWT token returned           |
| GET /api/auth/me      | ✅ Working | User info with Bearer token  |
| GET /locations        | ✅ Working | Returns 319 active locations |

**Sample Response (Locations):**

```json
[
  {"location_key": 225, "name": "MB A FAMOSA", "address": null},
  {"location_key": 74, "name": "MB AIR BIRU", "address": null},
  ...
]
```

API is now fully functional with cloud database connection.

---

## 6. Sync Sales API Implementation

### Objective

Create a **Sync Sales API** endpoint (`GET /apps/v2/sync/sales`) that mimics the original Xilnex API response structure, allowing the new system to serve as a drop-in replacement for legacy integrations.

### Implementation Details

- **Endpoint:** `GET /apps/v2/sync/sales`
- **Router:** `routers/sync_sales.py`
- **Source Tables:** 1:1 Replica tables (`dbo.com_5013_APP_4_SALES`, `APP_4_SALESITEM`, `APP_4_PAYMENT`, `APP_4_CUSTOMER`).
- **Pagination:** Uses `starttimestamp` (based on `UPDATE_TIMESTAMP` rowversion hex) and `limit` (max 1000).
- **Response Structure:** Matches `docs/sync_sales_api_response_schema.json` (Xilnex format).

### Key Features

1.  **Data Mapping:**
    - **Sales:** Joined with Items, Payments, and Customers.
    - **Timestamps:** Formatted to `YYYY-MM-DDTHH:MM:SS.000Z`.
    - **Outlet Info:** Mapped from `STRING_EXTEND_2` (ID) and `STRING_EXTEND_1` (Code).
    - **Status:** Mapped from `SALES_STATUS` and `PAYMENT_STATUS`.
2.  **Logic:**
    - `totalQuantity`: Sum of item quantities.
    - `lastTimestamp`: Returns the maximum `UPDATE_TIMESTAMP` from the batch (or input `starttimestamp` if no results).
    - `paxNumber`: Set to `null` (no reliable source).
    - **Payments:** Default to `Saved` status; `Void` if `BOOL_ISVOID` is true. Outlet falls back to sale location if missing.
3.  **Compatibility:** Accepts `mode` parameter (ignored) for backward compatibility.

### Verification

- **Compilation:** `python -m py_compile routers/sync_sales.py main.py` (Successful).
- **Integration:** Wired into `main.py`.
- **Testing Status:** Implemented but not yet runtime tested.

### Usage

```http
GET /apps/v2/sync/sales?limit=1000&starttimestamp=0x0000000000000000
Authorization: Bearer <token>
```

### Refinement: Date-Based Filtering (5 Dec 2025, 3:55 PM)

Modified the API to use **date-based filtering** instead of rowversion/timestamp pagination, as per user request.

- **Change:** Replaced `starttimestamp` parameter with `start_date` (YYYY-MM-DD).
- **Filtering:** Queries `dbo.com_5013_APP_4_SALES` where `DATETIME__SALES_DATE >= :start_date`.
- **Ordering:** `DATETIME__SALES_DATE ASC`, `ID ASC`.
- **Response:** `lastTimestamp` is now returned as `null`.
- **Usage:**
  ```

  ```

### Fixes: Outlet Name & Remark (5 Dec 2025, 4:50 PM)

Implemented fixes to align the API response with the schema:

1.  **Outlet Name:**
    - Joined `dbo.com_5013_APP_4_SALES` with `dbo.com_5013_LOCATION_DETAIL` on `SALE_LOCATION = ID`.
    - Mapped `outlet` field to `LOCATIONNAME` (human-readable name) instead of the UUID.
    - Fallback: Uses UUID if name lookup fails.
2.  **Remark Handling:**
    - Updated logic to return `null` (JSON null) instead of `""` (empty string) when remarks are empty or whitespace.
    - Applied to both sales and item-level remarks.
