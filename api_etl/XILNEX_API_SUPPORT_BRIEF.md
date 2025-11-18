# Xilnex API Support Brief
**Date:** 5 November 2025  
**Project:** Marry Brown Data Replication  
**Purpose:** Request for Xilnex technical support and API documentation

---

## 1) Project Briefing
**Objective:** Maintain an organisation‑controlled, read‑only copy of Xilnex operational data for reporting, analytics, audit, and business continuity purposes.

**Approach:** Replicate sales transactions and key reference data via official Xilnex Sync APIs on a scheduled basis. Provide a lightweight internal fallback portal for continuity if primary reporting tools are unavailable.

**Scope:** Read‑only integration; no writes to Xilnex system. Strictly compliant with usage terms.

---

## 2) Current Situation and Challenges

### What We've Experienced:

1. **Outdated Database Documentation**
   - The database structure documentation provided earlier is now outdated due to maintenance changes by Xilnex
   - Cannot reliably map tables/columns to business requirements
   - Makes direct database ETL approach unreliable

2. **Direct Database ETL Accuracy Issues**
   - Without current schema knowledge, our Direct‑DB extraction cannot consistently match figures shown in the Xilnex portal
   - Data reconciliation is difficult and time‑consuming
   - Cannot verify 100% accuracy

3. **Sales Sync API - Positive Progress**
   - The newly provided Sales Sync API (`/apps/v2/sync/sales`) is a significant improvement
   - Better structured data and higher accuracy than Direct‑DB approach
   - Successfully implemented and currently in use
   - **However:** Totals still don't always match the Xilnex portal (need to investigate remaining gaps)

4. **API Performance and Throughput Limitations**
   - **Response Time:** ~15 seconds per API call (can be slower during peak times)
   - **Data per Call:** Limited records per timestamp batch
   - **Historical Backfill:** Very time‑consuming to replicate data from database start (Oct 2018) to current date
   - Example: Pulling 7+ years of historical data requires thousands of sequential API calls

5. **Current Focus: Sales Data Only**
   - All efforts are currently focused on sales transactions
   - Other data domains (inventory, stock movements, products, etc.) are not yet integrated
   - Need similar sync APIs for other data types

---

## 3) Assistance Requested from Xilnex

### Priority Requests:

1. **Up‑to‑Date Database Structure Documentation**
   - Complete, current database schema for our Marry Brown tenant
   - Tables, columns, data types, relationships, constraints
   - Field descriptions and enumerations
   - **This would be the BEST solution**

2. **API Field Mapping Documentation** (if #1 not available)
   - Clear mapping showing how Sync Sales API fields reconcile to Xilnex portal reports
   - Column‑by‑column explanation including:
     - Rounding rules
     - Discount calculations (item‑level vs bill‑level)
     - Tax calculations (GST/SST handling)
     - Voids/returns/cancellations representation
     - Service charge application
     - Business date vs transaction date rules

3. **Portal Report Logic Documentation**
   - For each portal report, document:
     - Which fields/tables are used
     - Calculation formulas
     - Filters and business rules
   - This will help us replicate portal reports accurately

4. **Additional Sync API Endpoints**
   - Beyond sales, provide sync APIs for:
     - Inventory / stock movements
     - Product catalog (items, categories, prices)
     - Outlets and locations
     - Staff / cashiers
     - Refunds / voids (if separate from sales)
     - Payment methods
     - Customers (if applicable)

5. **API Performance Improvements**
   - **Higher throughput:** Larger batch sizes or faster response times
   - **Bulk export option:** Initial historical data dump (e.g., CSV/JSON export for Oct 2018 - present)
   - **Async bulk jobs:** Trigger a large date range export, poll for completion, download results
   - **Parallel requests:** Guidance on safe concurrency levels (how many simultaneous API calls allowed)

6. **Alternative Replication Methods**
   - Scheduled data dumps to secure storage (SFTP, cloud storage)
   - Database replication/read replica access
   - Reporting database snapshot
   - CDC (Change Data Capture) stream
   - Any faster approach than sequential API calls

---

## 4) API Success Evidence

### Current API Implementation

**Endpoint:** `https://api.xilnex.com/apps/v2/sync/sales`

**Authentication:** Headers-based (appid, token, auth level)

**Pagination:** Timestamp-based (not page numbers)
- First call: no parameters
- Response includes `lastTimestamp` (e.g., hex value)
- Subsequent calls: use `?starttimestamp=<value>`
- Continue until response is empty

### Test Results (November 5, 2025)

```
API Call Performance:
- Response Status: 200 (Success)
- Response Time: ~15 seconds per call
- Records per Batch: Variable (typically hundreds to thousands)
```

### Sample API Response Structure

```json
{
  "items": [
    {
      "id": 80289,
      "dateTime": "2018-10-04T14:33:26.000Z",
      "businessDateTime": "2018-10-04T00:00:00.000Z",
      "outlet": "MB IOI KULAI",
      "outletId": "57c63b36878141a8a226fb5de9f49d4c",
      "clientName": "CASH",
      "cashier": " DORA",
      "salesPerson": " DORA",
      "salesType": "Dine In",
      "orderNo": "1288",
      "paxNumber": "1",
      "status": "Completed",
      "paymentStatus": "PAID",
      "netAmount": 32.85,
      "grandTotal": 32.85,
      "gstTaxAmount": 1.86,
      "rounding": -0.01,
      "cost": 0.0,
      "profit": 31.0,
      "items": [
        {
          "id": 801364,
          "salesDate": "2018-10-04T00:00:00.000Z",
          "itemId": 101553,
          "itemCode": "KBC001",
          "itemName": "Korean Burger Combo",
          "unitPrice": 15.5,
          "quantity": 1.0,
          "subtotal": 15.5,
          "gstPercentage": 6.0,
          "gstAmount": 0.93,
          "category": "PROMOTIONS",
          "salesType": "Dine In"
        }
      ],
      "collection": [
        {
          "amount": 32.85,
          "method": "cash",
          "paymentDate": "2018-10-04T14:33:26.000Z",
          "tenderAmount": 32.85,
          "change": 0.0
        }
      ]
    }
  ],
  "key": null,
  "status": null,
  "hasSales": null,
  "journalId": 0
}
```

### Key Fields We're Working With:

**Transaction Level:**
- `id`, `dateTime`, `businessDateTime`
- `outlet`, `outletId`, `orderNo`
- `netAmount`, `grandTotal`, `gstTaxAmount`, `rounding`
- `status`, `paymentStatus`

**Item Level:**
- `itemId`, `itemCode`, `itemName`
- `unitPrice`, `quantity`, `subtotal`
- `gstPercentage`, `gstAmount`
- `category`, `salesType`

**Payment Level:**
- `method`, `amount`, `tenderAmount`, `change`

### Questions on Data Reconciliation:

1. **Rounding:** How is `rounding` calculated? When comparing portal totals, should we use `netAmount` or `grandTotal`?

2. **GST/Tax:** How to handle `gstInclusive` flag? Some items show `gstAmount: 0` but have `unitPrice: 0` (combo sub-items).

3. **Combo Items:** Items with `unitPrice: 0` and `pcid` linking to parent - should these be included or excluded from sales totals?

4. **Timestamp Pagination:** Does `lastTimestamp` guarantee we get ALL updates, even if records are modified after initial sync?

5. **Business Date vs Transaction Date:** Which should be used for daily sales reports matching portal?

---

## 5) Contact Information

**Project Lead:** YONG WERN JIE  
**Organization:** Marry Brown (Malaysia)  
**Email:** [TO BE PROVIDED]  
**Phone:** [TO BE PROVIDED]

---

## 6) Expected Outcome

With proper documentation and support from Xilnex, we aim to:

1. ✅ Achieve 100% accuracy match with Xilnex portal reports
2. ✅ Replicate all historical data efficiently (2018 - present)
3. ✅ Expand beyond sales to inventory and other data
4. ✅ Maintain real‑time or near‑real‑time sync
5. ✅ Provide business continuity and audit trail

---

**Thank you for your support!**

We appreciate Xilnex's partnership and look forward to your guidance on the above points.

