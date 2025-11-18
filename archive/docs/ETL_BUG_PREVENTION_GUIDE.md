# ETL Pipeline Bug Prevention Guide

**Based on Week 8 Bug Fixes (November 10-14, 2025)**

This guide documents critical bugs that were fixed and how to prevent them from happening again.

---

## 🐛 BUG #1: Date Parsing Exclusion (99% Data Loss)

### **What Happened:**
- ETL was filtering out 99% of records due to date parsing failures
- API returned ISO format dates (`dateTime`, `businessDateTime`, `salesDate`) but parser couldn't handle them
- Records with unparseable dates were marked "NO_DATE" and excluded from processing

### **Root Cause:**
- Single-format date parser that didn't match API's actual date field names
- No fallback mechanism when primary date field was missing

### **Fix Applied:**
- Implemented multi-format ISO date parser with fallback candidates:
  1. `dateTime` (main sales record)
  2. `businessDateTime` (from items, fallback)
  3. `salesDate` (from items, fallback)
- Each candidate tries multiple ISO formats: `%Y-%m-%dT%H:%M:%S.%fZ` and `%Y-%m-%dT%H:%M:%SZ`

### **Prevention Checklist:**
- ✅ **ALWAYS** analyze actual API response structure before writing date parsers
- ✅ **ALWAYS** implement multi-format date parsing with fallback candidates
- ✅ **ALWAYS** test with sample API responses to verify date parsing works
- ✅ **ALWAYS** log date parsing failures and count excluded records
- ✅ **NEVER** assume API date format matches documentation
- ✅ **NEVER** use single-format date parser without fallbacks

### **Code Pattern:**
```python
def parse_sale_datetime(sale: dict):
    """Multi-format date parser with fallback candidates."""
    candidates = [
        ("dateTime", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),
        ("businessDateTime", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),
        ("salesDate", ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]),
    ]
    for key, fmts in candidates:
        value = sale.get(key)
        if not value:
            continue
        for fmt in fmts:
            try:
                return dt.strptime(value, fmt)
            except Exception:
                continue
    return None
```

---

## 🐛 BUG #2: MERGE Statement Column Mismatches

### **What Happened:**
- MERGE statements were using API field names instead of staging schema column names
- This caused no-match updates and failed upserts
- Data wasn't loading to staging tables correctly

### **Root Cause:**
- MERGE statements joined/mapped on API field names (e.g., `dateTime`, `outlet`) instead of staging columns (e.g., `BusinessDateTime`, `OutletName`)
- Column mapping was inconsistent between extraction and MERGE operations

### **Fix Applied:**
- Created single source of truth for column mapping in `batch_insert_dataframe()`
- All API field names mapped to staging schema columns BEFORE database operations
- MERGE statements now use correct staging column names

### **Prevention Checklist:**
- ✅ **ALWAYS** maintain a single column mapping dictionary (API field → staging column)
- ✅ **ALWAYS** map API fields to staging columns BEFORE any database operations
- ✅ **ALWAYS** verify MERGE statements use staging column names, not API field names
- ✅ **ALWAYS** test MERGE operations with sample data to ensure correct mapping
- ✅ **NEVER** use API field names directly in MERGE statements
- ✅ **NEVER** have multiple mapping locations (causes inconsistencies)

### **Code Pattern:**
```python
# Single source of truth for column mapping
column_mapping = {
    'dbo.staging_sales': {
        'id': 'SaleID',
        'dateTime': 'BusinessDateTime',
        'outlet': 'OutletName',
        # ... all mappings here
    }
}

# Apply mapping BEFORE database operations
df_filtered = df_filtered.rename(columns=cols_to_rename)
```

---

## 🐛 BUG #3: Complex Field Serialization Performance

### **What Happened:**
- ETL was serializing ALL fields to JSON, even simple strings/numbers
- This caused severe performance bottlenecks (15+ minutes per chunk)
- Processing was extremely slow due to unnecessary serialization

### **Root Cause:**
- Serialization logic applied to all fields instead of only complex types (dict/list)
- No type checking before serialization

### **Fix Applied:**
- Only serialize complex fields (dict/list) to JSON
- Use pandas vectorization for batch processing instead of Python loops
- Reduced processing time from 15+ minutes to 5-10 seconds per chunk (375x improvement)

### **Prevention Checklist:**
- ✅ **ALWAYS** check field type before serialization (only serialize dict/list)
- ✅ **ALWAYS** use pandas vectorization for batch operations
- ✅ **ALWAYS** profile performance before and after optimization
- ✅ **NEVER** serialize simple types (strings, numbers, booleans)
- ✅ **NEVER** use Python loops for large datasets (use pandas vectorization)

### **Code Pattern:**
```python
# Only serialize complex fields (dict/list)
for col in df.columns:
    sample_values = df[col].dropna().head(100)
    if any(isinstance(val, (dict, list)) for val in sample_values):
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
```

---

## 🐛 BUG #4: Schema Column Size Truncation

### **What Happened:**
- Database columns were too small (NVARCHAR(50), NVARCHAR(200)) for actual API data
- Complex JSON fields (Items, Collection, Client) were stored as NVARCHAR(500) instead of MAX
- Data truncation errors: "String data, right truncation: length 520 buffer 500"

### **Root Cause:**
- Schema designed without analyzing actual API data sizes
- Complex fields (JSON) not using NVARCHAR(MAX)
- Schema changes not reflected due to connection pooling cache

### **Fix Applied:**
- Increased all regular NVARCHAR columns to 500 characters
- Changed complex fields (Items, Collection, Voucher, etc.) to NVARCHAR(MAX)
- Disabled SQLAlchemy connection pooling to ensure fresh schema reads
- Rebuilt staging tables from scratch with correct schema

### **Prevention Checklist:**
- ✅ **ALWAYS** analyze actual API data to determine column sizes
- ✅ **ALWAYS** use NVARCHAR(MAX) for complex/JSON fields
- ✅ **ALWAYS** use NVARCHAR(500) minimum for regular string fields
- ✅ **ALWAYS** disable connection pooling (NullPool) when schema changes
- ✅ **ALWAYS** dispose engine after schema changes: `engine.dispose()`
- ✅ **ALWAYS** verify schema matches actual data requirements
- ✅ **NEVER** assume column sizes without analyzing data
- ✅ **NEVER** use small NVARCHAR sizes (50, 100) for API data

### **SQL Pattern:**
```sql
-- Regular fields: NVARCHAR(500)
ALTER TABLE dbo.staging_sales ALTER COLUMN OutletName NVARCHAR(500);

-- Complex/JSON fields: NVARCHAR(MAX)
ALTER TABLE dbo.staging_sales ALTER COLUMN Items NVARCHAR(MAX);
ALTER TABLE dbo.staging_sales ALTER COLUMN Collection NVARCHAR(MAX);
```

### **Python Pattern:**
```python
# Disable connection pooling for fresh schema reads
from sqlalchemy.pool import NullPool
engine = create_engine(DATABASE_URL, poolclass=NullPool)

# Dispose engine after schema changes
engine.dispose()
```

---

## 🐛 BUG #5: Connection Pooling Schema Cache

### **What Happened:**
- SQLAlchemy connection pooling cached old schema definitions
- Schema changes (column size increases) weren't reflected in ETL operations
- Truncation errors persisted even after schema was fixed

### **Root Cause:**
- SQLAlchemy connection pool reused connections with cached metadata
- Schema changes not visible until pool was cleared

### **Fix Applied:**
- Disabled connection pooling using `NullPool`
- Added `engine.dispose()` call in `batch_insert_dataframe()` for fresh connections
- Rebuilt staging tables from scratch to ensure clean state

### **Prevention Checklist:**
- ✅ **ALWAYS** use `NullPool` when schema changes are frequent
- ✅ **ALWAYS** call `engine.dispose()` after schema modifications
- ✅ **ALWAYS** verify schema changes are reflected before running ETL
- ✅ **ALWAYS** rebuild tables from scratch after major schema changes
- ✅ **NEVER** rely on connection pool to reflect schema changes immediately

### **Code Pattern:**
```python
from sqlalchemy.pool import NullPool

# Disable pooling for fresh schema reads
engine = create_engine(DATABASE_URL, poolclass=NullPool)

# In batch operations, dispose to ensure fresh schema
def batch_insert_dataframe(conn, df, table_name):
    try:
        conn.engine.dispose()
    except:
        pass
    # ... rest of batch insert
```

---

## 🐛 BUG #6: Missing Field Mappings

### **What Happened:**
- API fields like `subtotal`, `costOfGoods`, `taxRate`, `batchId` weren't mapped to staging schema
- Missing fields caused NULL values or default assignments
- Data quality issues due to unmapped fields

### **Root Cause:**
- Incomplete field mapping between API and staging schema
- No systematic verification of all API fields

### **Fix Applied:**
- Systematically mapped all missing API fields to staging schema
- Added proper defaults for missing fields (costOfGoods, taxRate, batchId, etc.)
- Verified all three staging tables (sales, items, payments) have complete mappings

### **Prevention Checklist:**
- ✅ **ALWAYS** create complete field mapping dictionary covering ALL API fields
- ✅ **ALWAYS** verify mapping completeness by comparing API response to staging schema
- ✅ **ALWAYS** add defaults for optional/missing fields
- ✅ **ALWAYS** test with sample API responses to catch unmapped fields
- ✅ **NEVER** assume all fields are mapped without verification

---

## 📋 General Prevention Best Practices

### **Before Writing ETL Code:**
1. ✅ Analyze actual API response structure (use `get_single_api_call.py`)
2. ✅ Verify database schema matches API data requirements
3. ✅ Create complete field mapping dictionary
4. ✅ Test date parsing with sample API responses
5. ✅ Profile performance with sample data

### **During Development:**
1. ✅ Use pandas vectorization instead of Python loops
2. ✅ Only serialize complex fields (dict/list), not all fields
3. ✅ Implement multi-format date parsing with fallbacks
4. ✅ Use single source of truth for column mappings
5. ✅ Disable connection pooling when schema changes

### **Before Production:**
1. ✅ Verify all API fields are mapped to staging schema
2. ✅ Test with full date range to catch date parsing issues
3. ✅ Verify schema column sizes accommodate actual data
4. ✅ Test MERGE operations with sample data
5. ✅ Profile performance and optimize bottlenecks
6. ✅ Validate data quality (no NULLs where unexpected)

### **Monitoring & Debugging:**
1. ✅ Log date parsing failures and excluded record counts
2. ✅ Log field mapping issues (unmapped fields)
3. ✅ Log truncation errors with column names and data lengths
4. ✅ Track processing time per chunk to detect performance regressions
5. ✅ Write debug output to file (`debug/etl_debug.txt` in project directory) for troubleshooting

---

## 🎯 Quick Reference: Critical Checks

Before running ETL, verify:

- [ ] Date parser handles all API date field formats with fallbacks
- [ ] All API fields mapped to staging schema columns
- [ ] MERGE statements use staging column names (not API field names)
- [ ] Schema column sizes accommodate actual data (500+ for strings, MAX for JSON)
- [ ] Connection pooling disabled (NullPool) if schema changes
- [ ] Only complex fields (dict/list) are serialized to JSON
- [ ] Pandas vectorization used instead of Python loops
- [ ] Sample API response tested to verify all mappings work

---

**Last Updated:** November 14, 2025  
**Author:** YONG WERN JIE  
**Status:** ✅ Production-Ready Prevention Guide

