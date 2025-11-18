# Xilnex Sync API Testing Guide

**Date:** October 27, 2025  
**Purpose:** Test and evaluate Xilnex Sync API for ETL pipeline replacement

---

## 🎯 Objectives

1. Understand API response format
2. Compare API fields with current warehouse schema
3. Determine if API can replace direct database queries
4. Test incremental sync functionality

---

## 🚀 Quick Start

### Step 1: Run the Test Script

```bash
cd "C:\Users\MIS INTERN\marrybrown_etl"
.\venv\Scripts\activate
python test_xilnex_sync_api.py
```

### Step 2: Check the Output

The script will:
- ✅ Test the `/sales` endpoint
- ✅ Show response structure
- ✅ Save full JSON response to file
- ✅ Analyze available fields

---

## 🔍 What to Look For

### 1. Authentication Success
Look for: `Status Code: 200`

**If you see 401/403:**
- Authentication failed
- May need different header format
- Check with Xilnex support

### 2. Response Structure

**Expected format:**
```json
{
  "items": [
    {
      "ID": "...",
      "INVOICE_ID": "...",
      "SALES_TYPE": "...",
      // ... more fields
    }
  ],
  "lastTimestamp": "0x00000000A333D6F1",
  "totalCount": 1000
}
```

### 3. Field Comparison

Compare API response with your current staging tables:

#### **For Sales (staging_sales):**
Current columns you need:
- `invoice_id` (VARCHAR)
- `sales_person_username` (VARCHAR)
- `cashier` (VARCHAR)
- `sales_type` (VARCHAR)
- `datetime__date` (DATE)
- `datetime__time` (TIME)
- `location_id` (UNIQUEIDENTIFIER)
- `customer_id` (UNIQUEIDENTIFIER)
- `double_total_amount` (DECIMAL)
- `double_total_tax_amount` (DECIMAL)
- `string_extend_3` (VARCHAR) - Terminal ID
- `sales_status` (VARCHAR)
- `subsales_type` (VARCHAR)

**Check:** Does the API return all these fields?

#### **For Sales Items (staging_sales_items):**
Current columns you need:
- `invoice_id` (VARCHAR)
- `product_id` (INT)
- `double_quantity` (DECIMAL)
- `double_price` (DECIMAL)
- `double_discount` (DECIMAL)
- `double_cost` (DECIMAL)
- `double_mgst_tax_amount` (DECIMAL)
- `double_total_tax_amount` (DECIMAL)

**Check:** Does the `/salesitems` endpoint return these?

#### **For Payments (staging_payments):**
Current columns you need:
- `invoice_id` (VARCHAR)
- `method` (VARCHAR)
- `double_amount` (DECIMAL)
- `string_extend_2` (VARCHAR) - Card Type
- `datetime__date` (DATE)

---

## 📊 API Endpoints to Test

Based on Xilnex documentation:

| Endpoint | Purpose | Priority |
|----------|---------|----------|
| `/sales` | Sales headers (invoices) | 🔴 HIGH |
| `/salesitems` | Line items | 🔴 HIGH |
| `/payments` | Payment methods | 🔴 HIGH |
| `/items` | Product master data | 🟡 MEDIUM |
| `/customers` | Customer master data | 🟡 MEDIUM |
| `/locations` | Store locations | 🟢 LOW |

---

## 🧪 Testing Scenarios

### Scenario 1: Initial Full Sync
```bash
# Test getting first batch of sales
python test_xilnex_sync_api.py
```

**What to check:**
- ✅ Does it return data?
- ✅ How many records per request?
- ✅ Is there a `lastTimestamp`?

### Scenario 2: Incremental Sync
```python
# After first sync, test with timestamp
# Modify the script to pass the timestamp from previous response
test_api_endpoint("sales", "/sales", start_timestamp="0x00000000A333D6F1")
```

**What to check:**
- ✅ Does it return only new/changed records?
- ✅ Can you detect which records were updated vs inserted?

### Scenario 3: Pagination
```bash
# Test if there's pagination for large datasets
# Keep calling with lastTimestamp until empty
```

---

## 📝 Documentation to Check

From Xilnex documentation link:
https://developers.xilnex.com/docs/xilnex-developers/beb99101b3573-sync-sales

**Key questions to answer:**
1. What are ALL available endpoints?
2. What query parameters are supported?
3. What's the rate limit (requests per minute)?
4. What's the max records per request?
5. How to handle errors/retries?

---

## ✅ Decision Criteria

**Use API if:**
- ✅ Returns all required fields for your warehouse
- ✅ Incremental sync works reliably
- ✅ Performance is good (faster than direct DB queries)
- ✅ Rate limits are acceptable for daily ETL
- ✅ Response format is consistent

**Stick with direct DB if:**
- ❌ Missing critical fields
- ❌ API is unreliable or slow
- ❌ Rate limits too restrictive
- ❌ Complex transformations needed that require joins

---

## 🔄 Next Steps After Testing

1. **Document API Response:**
   - Save sample responses for all endpoints
   - Create field mapping document (API → Warehouse)

2. **Design New ETL:**
   - Replace `extract` functions with API calls
   - Implement timestamp tracking (per endpoint)
   - Add error handling and retry logic

3. **Test Incremental Logic:**
   - Run full sync on test date (e.g., Oct 1)
   - Run incremental sync for Oct 2-3
   - Verify only new/changed records appear

4. **Performance Comparison:**
   - Time: API vs Direct DB query
   - Data completeness check
   - Accuracy validation

---

## 🐛 Troubleshooting

### Issue: 401 Unauthorized
**Solutions:**
- Check if credentials are correct
- Try different header format:
  ```python
  headers = {
      "Authorization": f"Bearer {TOKEN}",
      "X-API-Key": APP_ID,
  }
  ```
- Contact Xilnex support for correct authentication method

### Issue: 404 Not Found
**Solutions:**
- Endpoint path might be different
- Try variations: `/salesitems` vs `/sales-items` vs `/salesitem`
- Check documentation for exact endpoint paths

### Issue: Empty Response
**Solutions:**
- Check date range (maybe no data in that period)
- Try without `starttimestamp` first (get everything)
- Verify company ID is correct in authentication

### Issue: Timeout
**Solutions:**
- Reduce batch size (if parameter exists)
- Check network connection
- API might be slow - compare with direct DB query time

---

## 📞 Contact

**Xilnex Support:** (if issues arise)
- Reference: COM_5013 - Marrybrown
- API Token: MB-db-replicate-api

---

**Last Updated:** October 27, 2025  
**Author:** YONG WERN JIE (MIS Intern)

