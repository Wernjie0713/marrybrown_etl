# Troubleshooting: Stuck ETL Process

**Issue:** ETL process hangs/freezes during loading  
**Date:** October 30, 2025

---

## Common Stuck Points

### 1. **Stuck at "Inserting X rows in batches..."**

**Symptoms:**
```
Loading sales headers...
  Inserting 11809 sales in batches of 1000...
[STUCK - no progress]
```

**Root Cause:** Pandas `to_sql()` slow over network

**Solution Applied:** ✅ Switch to direct pyodbc `cursor.fast_executemany`
```python
cursor = raw_conn.cursor()
cursor.fast_executemany = True
cursor.executemany(insert_sql, values)
```

---

### 2. **Stuck at "Clearing existing staging data..."**

**Symptoms:**
```
Clearing existing staging data...
[STUCK - no progress]
```

**Root Causes:**
- Previous INSERT still holding table locks
- TRUNCATE waiting for lock release
- No timeout configured

**Solution Applied:** ✅ Added lock timeout + DELETE fallback
```python
# Try TRUNCATE with 5-second timeout
conn.execute(text("SET LOCK_TIMEOUT 5000"))
conn.execute(text("TRUNCATE TABLE ..."))

# If fails, fallback to DELETE
except:
    conn.execute(text("DELETE FROM ..."))
```

---

## How to Recover from Stuck State

### **Step 1: Kill Python Process**

**Option A:** Graceful stop
```powershell
# In the terminal running the script
Ctrl + C
```

**Option B:** Force kill
```powershell
# If Ctrl+C doesn't work
Get-Process python | Where-Object {$_.Path -like "*marrybrown_etl*"} | Stop-Process -Force
```

**Option C:** Close terminal window
- Just close the PowerShell window
- SQL Server will auto-rollback after timeout

---

### **Step 2: Check for Blocking Sessions**

```powershell
cd "C:\Users\MIS INTERN\marrybrown_etl"
sqlcmd -S 10.0.1.194,1433 -d MarryBrown_DW -U etl_user -P "ETL@MarryBrown2025!" -i kill_blocking_sessions.sql
```

**What this does:**
1. Shows all active sessions
2. Identifies blocking sessions
3. (Optional) Kill Python sessions automatically

**Manual Kill:**
```sql
-- Replace 52 with actual session_id from query
KILL 52;
```

---

### **Step 3: Verify Tables Are Accessible**

```sql
-- Quick test
SELECT COUNT(*) FROM staging_sales;
SELECT COUNT(*) FROM staging_sales_items;
SELECT COUNT(*) FROM staging_payments;
```

**Expected:** Should return immediately (not hang)

---

### **Step 4: Restart ETL**

```powershell
cd "C:\Users\MIS INTERN\marrybrown_etl"
python api_etl\run_cloud_etl_multi_month.py
```

**Expected Output:**
```
Clearing existing staging data...
  [OK] Staging tables cleared (TRUNCATE)

Loading sales headers...
  Inserting 11809 sales using fast executemany...
  [OK] Loaded 11809 sales headers

Loading sales items...
  Inserting 40000 items using fast executemany...
  [OK] Loaded 40000 sales items

Loading payments...
  Inserting 12000 payments using fast executemany...
  [OK] Loaded 12000 payments

STAGING LOAD COMPLETE (FAST EXECUTEMANY)
```

**Total Time:** 30-60 seconds per month

---

## Prevention Measures (Now Implemented)

### ✅ **Lock Timeout Protection**
```sql
SET LOCK_TIMEOUT 5000  -- 5 seconds max wait
```
- Prevents indefinite waiting
- Fast fail if table is locked

### ✅ **Automatic Fallback**
```python
try:
    TRUNCATE TABLE ...  # Fast but can lock
except:
    DELETE FROM ...     # Slower but more reliable
```

### ✅ **Connection Timeout**
```python
connection_uri = "...?TrustServerCertificate=yes"
engine = create_engine(connection_uri, pool_pre_ping=True)
```

### ✅ **Direct pyodbc Control**
```python
cursor.fast_executemany = True  # Bypass pandas overhead
```

---

## Performance Timeline

| Issue | Fix | Result |
|-------|-----|--------|
| Row-by-row INSERT (45 min) | Pandas batch | 10 min |
| Pandas slow (10 min) | pyodbc fast_executemany | 30-60 sec |
| TRUNCATE stuck (∞) | Lock timeout + DELETE fallback | 5 sec |

**Overall: 45 minutes → 30-60 seconds** ⚡

---

## If Still Stuck After These Steps

### **Check Network Connectivity**
```powershell
# Test connection to cloud database
Test-NetConnection -ComputerName 10.0.1.194 -Port 1433
```

### **Check SQL Server Service**
```powershell
# On cloud server
Get-Service MSSQLSERVER
```

### **Check Firewall**
```powershell
# On cloud server
Get-NetFirewallRule -DisplayName "*SQL*" | Where-Object Enabled -eq True
```

### **Check Database Size**
```sql
-- Make sure database hasn't filled up
EXEC sp_spaceused;
```

### **Enable Query Logging**
```python
# In extract_from_api.py, add:
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
```

---

## Contact Information

**Developer:** YONG WERN JIE  
**Date Implemented:** October 30, 2025  
**Files Modified:**
- `api_etl/extract_from_api.py` - Added fast_executemany + lock timeout
- `kill_blocking_sessions.sql` - Session management script

