# Chunked ETL Approach - Guide

## Overview

The **chunked approach** extracts from API and loads to staging **incrementally** (every 50 API calls), rather than extracting all data first then loading.

## Why Chunked Approach?

### Problem with Original Approach
```
Extract 800 calls (25 min) → Load all at once → ❌ If fails, lost all 25 minutes!
```

### Solution with Chunked Approach
```
Extract 50 calls → Load ✓ (saved!)
Extract 50 calls → Load ✓ (saved!)
Extract 50 calls → Load ✓ (saved!)
...
❌ If fails at call 700 → Still have 650 calls saved!
```

## Key Benefits

| Feature | Benefit |
|---------|---------|
| **Progress Preservation** | Every 50 calls = automatic checkpoint |
| **Memory Efficiency** | Clear memory after each chunk |
| **Early Failure Detection** | Know if DB connection broken after 50 calls, not 800 |
| **Crash Recovery** | Resume from last chunk, not from scratch |
| **No "Extract All Then Fail" Risk** | Data saved during extraction, not after |

## Performance Impact

**Speed Difference:** Minimal (~1-2 minutes slower for 15 months)

| Approach | Speed | Safety |
|----------|-------|--------|
| All-at-once | ~27 min | ❌ All lost if fails |
| Chunked | ~28 min | ✅ Progress preserved |

**Worth it? YES!** 3% slower for 100% safer.

## How It Works

### Configuration

**File:** `config_api.py`
```python
MAX_API_CALLS = 500  # Testing limit (good for 3-4 months)
# Set to None for unlimited (full production)
```

**File:** `run_cloud_etl_chunked.py`
```python
CHUNK_SIZE = 50      # Save every 50 API calls (~50K records)
START_DATE = "2018-10-01"
END_DATE = "2018-12-31"  # 3 months for testing
```

### Execution Flow

1. **Extract 50 API calls** (~50K records, ~1.5 min)
2. **Load to staging** via MERGE (~30 sec)
3. **Clear memory**
4. **Repeat** until date range covered
5. **Transform to facts** (all at once at end)

### Progress Display

```
[Call 10] 2018-10-01 to 2018-10-03 | 1000 records | Total: 10,000
[Call 20] 2018-10-04 to 2018-10-07 | 1000 records | Total: 20,000
...
[Call 50] 2018-10-15 to 2018-10-18 | 1000 records | Total: 50,000

================================================================================
[CHECKPOINT 1] Saving chunk after 50 API calls...
================================================================================
  [CHUNK 1] Loading 50,000 sales to staging...
  [CHUNK 1] Filtered to 48,500 sales in date range
  [CHUNK 1] ✓ Loaded: 48,500 sales, 245,000 items, 48,500 payments
[CHECKPOINT 1] ✓ Progress saved! Memory cleared.
  Total so far: 48,500 sales, 245,000 items

[Call 51] 2018-10-19 to 2018-10-21 | 1000 records | Total: 1,000
...
```

## Usage

### Step 1: Clear Previous Data (Optional)

If you want fresh start:
```bash
cd C:\laragon\www\marrybrown_etl
python api_etl\clear_etl_data.py
# Type "YES" to confirm
```

### Step 2: Run Chunked ETL

```bash
cd C:\laragon\www\marrybrown_etl
python api_etl\run_cloud_etl_chunked.py
```

### Step 3: Monitor Progress

Watch for checkpoint messages every 50 calls:
- `[CHECKPOINT 1]` = First 50K records saved
- `[CHECKPOINT 2]` = Next 50K records saved
- etc.

### Step 4: If Interrupted

**No problem!** Your progress is saved. Just run again:
```bash
python api_etl\run_cloud_etl_chunked.py
```

The MERGE logic will skip duplicates automatically.

## Configuration Recommendations

### For Initial Testing (Recommended)
```python
# config_api.py
MAX_API_CALLS = 500  # ~500K records

# run_cloud_etl_chunked.py
START_DATE = "2018-10-01"
END_DATE = "2018-12-31"  # 3 months
CHUNK_SIZE = 50
```

**Expected:**
- ~150 API calls (smart exit stops early)
- 3 checkpoints saved
- ~5-7 minutes total

### For Full Production (After Testing)
```python
# config_api.py
MAX_API_CALLS = None  # Unlimited

# run_cloud_etl_chunked.py
END_DATE = "2019-12-31"  # Full 15 months
CHUNK_SIZE = 50
```

**Expected:**
- ~500-800 API calls
- 10-16 checkpoints saved
- ~40-60 minutes total

## Troubleshooting

### Q: What if ETL crashes at call 700?

**A:** You still have 650 calls (13 chunks) saved in staging! Just run again and it will continue from where you left off (MERGE prevents duplicates).

### Q: How much memory does chunked approach use?

**A:** Maximum 50K records in memory at any time (~50MB), then cleared. Much better than 800K records (~800MB) for all-at-once approach.

### Q: Can I resume from a failed run?

**A:** Yes! Just run the script again. The MERGE logic automatically:
- Skips records already in staging
- Updates changed records
- Adds new records only

### Q: What if I want to stop and resume later?

**A:** Press `Ctrl+C` to stop safely. Progress up to last checkpoint is saved. Run again later to continue.

## Multi-Worker Question

**Q: Can we use multiple workers to extract faster?**

**A: NO** - Not possible with this API because:
1. ❌ API only gives you ONE timestamp at a time
2. ❌ You can't know future timestamps to parallelize
3. ❌ Must be sequential: Call 1 → get timestamp → Call 2 → get timestamp...
4. ✅ This is **serial pagination by design**

**Only way to parallelize would be:**
- If API supported date range parameters (it doesn't)
- If you knew timestamps beforehand (you don't)

**Conclusion:** Sequential extraction with chunked loading is optimal.

## Files Created

1. **`extract_from_api_chunked.py`** - Chunked extraction logic
2. **`run_cloud_etl_chunked.py`** - Chunked orchestrator
3. **`CHUNKED_ETL_GUIDE.md`** - This guide

## Next Steps

1. ✅ Run with 3-month range first (testing)
2. ✅ Verify data in warehouse
3. ✅ If successful, extend to full 15 months
4. ✅ Set `MAX_API_CALLS = None` for unlimited
5. ✅ Run full production extraction

---

**Author:** YONG WERN JIE  
**Date:** November 7, 2025  
**Status:** Ready for testing

