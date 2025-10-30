# Xilnex Sync API to Warehouse Schema Mapping

**Date:** October 28, 2025  
**Purpose:** Complete field-by-field mapping analysis to determine ETL transformation logic and identify missing/useful fields

---

## 📋 Executive Summary

### API Data Structure
- **Sale Header:** 78 fields (transaction-level data)
- **Sale Items:** 69 fields per line item (product-level data)
- **Payments (Collection):** 39 fields per payment (payment-level data)
- **Nested Structure:** One sale contains multiple items and multiple payments

### Current Warehouse Coverage
- ✅ **Well Covered:** Basic transaction data (amounts, dates, quantities)
- ⚠️ **Partially Covered:** Some fields exist in API but not captured
- ❌ **Not Covered:** Advanced features (shipping, external orders, custom fields)

---

## 🔄 MAPPING ANALYSIS

## Part 1: SALE HEADER (Transaction Level)

### ✅ **Fields Currently Captured in Warehouse**

| API Field | Warehouse Table | Warehouse Column | Notes |
|-----------|----------------|------------------|-------|
| `id` | fact_sales_transactions | `SaleNumber` | Sale transaction ID |
| `dateTime` | fact_sales_transactions | `DateKey`, `TimeKey` | Split into date and time dimensions |
| `businessDateTime` | fact_sales_transactions | `DateKey` | Business date (more accurate than system datetime) |
| `outlet` | dim_locations | `LocationName` | Outlet name lookup |
| `outletId` | dim_locations | `LocationGUID` | Outlet identifier |
| `cashier` | dim_staff | `StaffFullName` | Cashier name |
| `salesType` | fact_sales_transactions | `SaleType` | 'Dine In', 'Take Away', etc. |
| `subSalesType` | fact_sales_transactions | `SubSalesType` | NEW in v1.8.0, sub-classification |
| `grandTotal` | fact_sales_transactions | `TotalAmount` | Final amount including tax |
| `netAmount` | fact_sales_transactions | `NetAmount` | Amount before tax |
| `gstTaxAmount` | fact_sales_transactions | `TaxAmount` | Tax amount |
| `paid` | fact_payments | `Amount` (sum) | Amount paid |
| `balance` | - | - | Calculated: `grandTotal - paid` |
| `status` | fact_sales_transactions | Implicit in existence | 'Saved' transactions only |
| `paymentStatus` | - | - | Not directly stored, derived from payments |
| `rounding` | - | - | Not currently captured (⚠️ MISSING) |
| `clientName` | dim_customers | `FullName` | Customer name (if loyalty member) |
| `paxNumber` | - | - | Number of people (⚠️ MISSING - could be useful) |

### ⚠️ **Useful Fields NOT Currently Captured**

| API Field | Data Type | Potential Use | Priority |
|-----------|-----------|---------------|----------|
| `rounding` | float | Accurate financial reconciliation | **HIGH** |
| `paxNumber` | str | Average spend per person, group size analysis | **MEDIUM** |
| `billDiscountAmount` | float | Bill-level discount tracking | **MEDIUM** |
| `totalBillDiscountAmount` | float | Total discount for reconciliation | **MEDIUM** |
| `discountPercentage` | float | Discount rate analysis | **LOW** |
| `orderNo` | str | Unique order number (different from sale ID) | **MEDIUM** |
| `reference` | str | External reference (e.g., delivery platform order ID) | **LOW** |
| `remark` | str | Transaction remarks/notes | **LOW** |
| `projectCode` | str | Project/campaign tracking | **LOW** |
| `totalQuantity` | float | Total items in sale (quick metric) | **LOW** |
| `cost` | float | Total COGS for sale (⚠️ but often zero in combos) | **HIGH** |
| `profit` | float | Sale-level profit (⚠️ but unreliable due to cost issue) | **MEDIUM** |

### ❌ **Advanced Fields Not Relevant (Skip)**

| API Field | Reason to Skip |
|-----------|----------------|
| `orderSource` | Null in most cases, redundant with `OrderSource` in items |
| `deliveryType` | Null in most cases, Marrybrown doesn't use delivery much |
| `shipmentDateTime`, `trackingNumber`, `trackingLink` | Shipping not applicable to F&B POS |
| `shippingAddress`, `billingAddress` | Not used in restaurant context |
| `cancelDateTime`, `cancelBy`, `cancelRemark` | Canceled sales not included in API |
| `pickupTime`, `orderStatus` | Online ordering fields (not used) |
| `foreignTotalAmount`, `exchangeRate`, `currencyCode` | Multi-currency not used |
| `customFieldValueOne` through `customFieldValueFive` | All NULL in sample data |
| `recipientName`, `recipientContact` | Not used |
| `voucher` (array) | Empty in sample, handled via promotions dimension |
| `extendedSales` (array) | Empty in sample |
| `term`, `salesOrderNo`, `externalReferenceId` | Not applicable to POS |

---

## Part 2: SALE ITEMS (Line Item Level)

### ✅ **Fields Currently Captured in Warehouse**

| API Field | Warehouse Table | Warehouse Column | Notes |
|-----------|----------------|------------------|-------|
| `id` | fact_sales_items | - | Line item ID (not directly stored) |
| `itemId` | dim_products | `SourceProductID` | Product master ID |
| `itemCode` | dim_products | `ProductCode` | Product SKU |
| `itemName` | dim_products | `ProductName` | Product display name |
| `category` | dim_products | `Category` | Product category |
| `brand` | dim_products | `Brand` | Product brand |
| `quantity` | fact_sales_transactions | `Quantity` | Units sold |
| `unitPrice` | - | - | Derived from `GrossAmount / Quantity` |
| `subtotal` | fact_sales_transactions | `GrossAmount` | Line total before discount |
| `discountAmount` | fact_sales_transactions | `DiscountAmount` | Discount applied |
| `gstAmount` | fact_sales_transactions | `TaxAmount` | Tax per line item |
| `totalTaxAmount` | fact_sales_transactions | `TaxAmount` | Total tax (same as gstAmount) |
| `taxCode` | - | - | Not stored (⚠️ MISSING - useful for tax reporting) |
| `gstPercentage` | - | - | Not stored (⚠️ MISSING - 6% or 0%) |
| `salesType` | fact_sales_transactions | `SaleType` | Item-level sale type |
| `salesitemSubsalesType` | fact_sales_transactions | `SubSalesType` | NEW in v1.8.0 |
| `salesPerson` | dim_staff | `StaffFullName` | Salesperson for this item |
| `businessDateTime` | fact_sales_transactions | `DateKey` | Business date |
| `salesDate` | fact_sales_transactions | `DateKey` | Sale date (redundant) |
| `cost` | fact_sales_transactions | `CostAmount` | ⚠️ Often zero for combos, calculated via recipes |
| `profit` | - | - | Calculated: `NetAmount - CostAmount` |

### ⚠️ **Useful Fields NOT Currently Captured**

| API Field | Data Type | Potential Use | Priority |
|-----------|-----------|---------------|----------|
| `taxCode` | str | Tax category tracking ('SV', 'ZR', 'SR') | **HIGH** |
| `gstPercentage` | float | Tax rate validation (6% vs 0%) | **MEDIUM** |
| `enterPrice` | float | Actual price entered (vs list price) | **LOW** |
| `foc` | bool | Free of charge items (important for promo analysis) | **HIGH** |
| `isServiceCharge` | bool | Service charge indicator | **MEDIUM** |
| `serviceChargePercentage` | float | Service charge rate | **MEDIUM** |
| `discountPercentage` | float | Discount rate per item | **LOW** |
| `discountRemark` | str | Reason for discount | **LOW** |
| `totalBillLevelDiscountAmount` | float | Portion of bill discount allocated to this item | **MEDIUM** |
| `voucherNumber` | str | Voucher used (if any) | **MEDIUM** |
| `promoGroup` | str | Promotion grouping | **LOW** |
| `model` | str | Product variant (e.g., 'REGULAR', 'LARGE') | **MEDIUM** |
| `stockType` | str | Stock type ('Normal', 'Package', etc.) | **LOW** |
| `isPrint` | bool | Whether item printed to kitchen | **LOW** |
| `salesItemRemark` | str | Item-specific remarks | **LOW** |
| `pcid` | str | Parent container ID (links item to sale header) | **CRITICAL for API ETL** |
| `itemIndex` | str | Item sequence in sale | **LOW** |

### ❌ **Fields Not Relevant (Skip)**

| API Field | Reason to Skip |
|-----------|----------------|
| `deliveryQuantity` | Not used in F&B POS |
| `orderSource`, `deliveryType` | Null in most cases |
| `advanceDiscount`, `priceSchemes` (object) | Complex, not needed for analytics |
| `customValueOne` through `customValueFifteen` | All NULL |
| `alternateLookup`, `scanCode`, `uniqueId` | Not used |
| `matrixId`, `matrixBarcode`, `matrixX`, `matrixY` | Matrix variants not used |
| `instoreOutlet` | Redundant with sale-level outlet |
| `subItems` | NULL (combo items not nested) |
| `description`, `itemType` | Redundant or empty |

---

## Part 3: PAYMENTS (Collection Level)

### ✅ **Fields Currently Captured in Warehouse**

| API Field | Warehouse Table | Warehouse Column | Notes |
|-----------|----------------|------------------|-------|
| `id` | fact_payments | - | Payment record ID (not stored) |
| `method` | dim_payment_types | `PaymentMethodName` | 'cash', 'card', 'ewallet', etc. |
| `amount` | fact_payments | `Amount` | Payment amount |
| `paymentDate` | fact_payments | Derived from sale DateKey/TimeKey | Payment timestamp |
| `businessDate` | fact_sales_transactions | `DateKey` | Business date |
| `declarationSessionId` | - | - | EOD session ID (⚠️ MISSING - useful for EOD reconciliation) |
| `eodLogId` | - | - | EOD log ID (⚠️ MISSING) |
| `outlet` | dim_locations | `LocationName` | Outlet where payment occurred |
| `reference` | - | - | Payment reference number (⚠️ MISSING - useful for tracing) |
| `tenderAmount` | - | - | Amount given by customer (⚠️ MISSING) |
| `change` | - | - | Change given (⚠️ MISSING) |
| `cardType`, `cardType2`, `cardType3` | fact_sales_transactions | `CardType` | Specific card type (VISA, MASTERCARD, etc.) |

### ⚠️ **Useful Fields NOT Currently Captured**

| API Field | Data Type | Potential Use | Priority |
|-----------|-----------|---------------|----------|
| `declarationSessionId` | str | EOD session tracking and reconciliation | **HIGH** |
| `eodLogId` | str | End-of-day log reference | **MEDIUM** |
| `reference` | str | Payment reference/transaction ID | **HIGH** |
| `tenderAmount` | float | Cash handling analysis (tender vs actual) | **MEDIUM** |
| `change` | float | Change calculation verification | **LOW** |
| `invoiceID` | str | Invoice number (may differ from sale number) | **MEDIUM** |
| `clientID` | str | Customer ID for this payment | **LOW** |
| `isDeposit` | bool | Deposit payment indicator | **LOW** |
| `isVoid` | bool | Voided payment indicator | **MEDIUM** |
| `status` | str | Payment status ('Paid', 'Pending', etc.) | **LOW** |
| `usedDate` | str | Date payment was used/cleared | **LOW** |
| `creditCardRate` | float | Card transaction fee rate | **LOW** |
| `availableBalance` | float | Remaining balance (for vouchers, cards) | **LOW** |

### ❌ **Fields Not Relevant (Skip)**

| API Field | Reason to Skip |
|-----------|----------------|
| `foreignAmount`, `exchangeRate`, `foreignGain`, `currencyCode` | Multi-currency not used |
| `prepaidCardNumber`, `prepaidReferenceNumber` | Prepaid cards not common |
| `cardAppCode`, `cardExpiry`, `cardLookup`, `traceNumber` | Technical card processing details (not for analytics) |
| `receivedBy`, `receivedByCashierName` | Redundant with cashier from sale header |
| `remark`, `remarks` | Null in most cases |
| `internalReferenceId`, `salesOrderId` | Not used |

---

## 🎯 CRITICAL FINDINGS

### 1. **Fields You MUST Capture (Currently Missing)**

| Field | Table | Reason | Impact |
|-------|-------|--------|--------|
| `rounding` (sale) | Add to fact_sales_transactions | Financial reconciliation accuracy | Rounding differences accumulate to thousands RM |
| `taxCode` (item) | Add to fact_sales_transactions or staging | Tax reporting compliance | Required for SST/GST reports |
| `foc` (item) | Add to fact_sales_transactions | Free item tracking | Promo effectiveness analysis |
| `declarationSessionId` (payment) | Add to fact_payments or staging | EOD reconciliation | Critical for cashier closing |
| `reference` (payment) | Add to fact_payments | Payment tracing | Audit trail for disputes |
| `pcid` (item) | Staging only (ETL linking) | Links item to sale header | Required for API ETL join logic |

**Recommendation:** Add these 6 fields to your schema before building API ETL!

---

### 2. **Fields That Would Be Nice to Have**

| Field | Use Case | Priority |
|-------|----------|----------|
| `paxNumber` | Average spend per person | Medium |
| `billDiscountAmount` | Discount analysis | Medium |
| `gstPercentage` | Tax rate validation | Medium |
| `isServiceCharge` | Service charge tracking | Medium |
| `model` | Product variant analysis (REGULAR vs LARGE) | Medium |
| `tenderAmount` / `change` | Cash handling audits | Low |
| `orderNo` | Order tracking | Low |

---

### 3. **API-Specific Challenges**

#### **A. Nested Structure**
- API returns hierarchical data: `sale → items[] → subItems[]` and `sale → collection[]`
- Warehouse uses flat tables with foreign keys
- **Solution:** Flatten during ETL with proper ID tracking

#### **B. Many-to-Many Relationships**
- One sale has multiple items (1:N) ✅ Already handled
- One sale has multiple payments (1:N) ✅ Already handled
- Split-tender: One item splits across multiple payments (M:N) ✅ Already handled

#### **C. Data Type Conversions**
| API Type | Warehouse Type | Conversion |
|----------|----------------|------------|
| ISO datetime string | DATE + TIME | Parse and split |
| `None` (null) | Various | Handle with COALESCE/-1 defaults |
| float (always) | DECIMAL(18,4) | Direct cast |
| nested objects | Foreign keys | Lookup dimension tables |

#### **D. ID Mapping Required**
| API Field | Dimension Lookup | Match On |
|-----------|-----------------|----------|
| `outletId` | dim_locations | LocationGUID |
| `itemId` | dim_products | SourceProductID |
| `cashier` (name) | dim_staff | StaffFullName |
| `method` | dim_payment_types | PaymentMethodName |
| `clientName` | dim_customers | FullName (optional) |

---

## 📝 TRANSFORMATION LOGIC (ETL Pseudocode)

### Step 1: Extract and Flatten API Response

```python
for sale in api_response['data']['sales']:
    # Extract sale header
    sale_id = sale['id']
    business_date = parse_date(sale['businessDateTime'])
    date_key = format_date_key(business_date)  # YYYYMMDD
    time_key = format_time_key(sale['dateTime'])  # HHMMSS
    
    # Lookup dimensions
    location_key = lookup_location(sale['outletId'])
    cashier_key = lookup_staff(sale['cashier'])
    
    # Extract payments
    for payment in sale['collection']:
        payment_type_key = lookup_payment_type(payment['method'])
        payment_amount = payment['amount']
        payment_reference = payment['reference']
        eod_session_id = payment['declarationSessionId']
        
        # Insert into fact_payments (or staging)
        insert_payment({
            'SaleNumber': sale_id,
            'PaymentTypeKey': payment_type_key,
            'Amount': payment_amount,
            'Reference': payment_reference,
            'EODSessionID': eod_session_id,
            ...
        })
    
    # Extract items
    for item in sale['items']:
        product_key = lookup_product(item['itemId'])
        
        # Calculate proportional payment allocation (for split-tender)
        item_net_amount = item['subtotal'] - item['discountAmount']
        item_total_amount = item_net_amount + item['totalTaxAmount']
        
        # For each payment, create a fact row with proportional amount
        for payment in sale['collection']:
            payment_proportion = payment['amount'] / sale['grandTotal']
            allocated_amount = item_total_amount * payment_proportion
            
            # Insert into fact_sales_transactions
            insert_fact({
                'DateKey': date_key,
                'TimeKey': time_key,
                'LocationKey': location_key,
                'ProductKey': product_key,
                'StaffKey': cashier_key,
                'PaymentTypeKey': payment_type_key,
                'SaleNumber': sale_id,
                'SaleType': item['salesType'],
                'SubSalesType': item['salesitemSubsalesType'],
                'Quantity': item['quantity'],
                'GrossAmount': item['subtotal'],
                'DiscountAmount': item['discountAmount'],
                'NetAmount': item_net_amount * payment_proportion,
                'TaxAmount': item['totalTaxAmount'] * payment_proportion,
                'TotalAmount': allocated_amount,
                'CostAmount': item['cost'] * payment_proportion,  # May be zero
                'CardType': get_card_type(payment),
                # NEW FIELDS
                'TaxCode': item['taxCode'],
                'IsFOC': item['foc'],
                'Rounding': sale['rounding'] * payment_proportion,
                ...
            })
```

### Step 2: Handle Special Cases

```python
# Returns: Skip payment allocation
if item['salesType'] == 'Return':
    # Use actual amounts, don't split across payments
    insert_fact_return({
        'TotalAmount': item_total_amount,  # Full amount, not split
        ...
    })

# Zero-cost items: Calculate from recipe costs (existing Parquet logic)
if item['cost'] == 0.0 and item_is_combo(item['itemId']):
    cost = calculate_recipe_cost(item['itemId'])
```

---

## 🚀 RECOMMENDED SCHEMA CHANGES

### Add Columns to `fact_sales_transactions`

```sql
ALTER TABLE dbo.fact_sales_transactions
ADD 
    TaxCode VARCHAR(10) NULL,           -- 'SV', 'ZR', 'SR'
    TaxRate DECIMAL(5,2) NULL,          -- 6.00, 0.00
    IsFOC BIT DEFAULT 0,                -- Free of charge flag
    Rounding DECIMAL(10,4) NULL,        -- Rounding adjustment
    Model VARCHAR(100) NULL,            -- Product variant (REGULAR, LARGE)
    IsServiceCharge BIT DEFAULT 0;      -- Service charge flag
```

### Add Columns to `fact_payments`

```sql
ALTER TABLE dbo.fact_payments
ADD 
    PaymentReference VARCHAR(255) NULL,  -- Payment transaction reference
    EODSessionID VARCHAR(50) NULL,       -- Cashier session ID
    TenderAmount DECIMAL(18,4) NULL,     -- Amount tendered
    ChangeAmount DECIMAL(18,4) NULL,     -- Change given
    IsVoid BIT DEFAULT 0;                -- Voided payment flag
```

### Create Staging Table for API Raw Data

```sql
CREATE TABLE dbo.staging_sales_api (
    SaleID BIGINT,
    BusinessDateTime DATETIME,
    OutletID VARCHAR(255),
    OutletName VARCHAR(255),
    CashierName VARCHAR(255),
    SalesType VARCHAR(50),
    SubSalesType VARCHAR(50),
    GrandTotal DECIMAL(18,4),
    NetAmount DECIMAL(18,4),
    TaxAmount DECIMAL(18,4),
    Paid DECIMAL(18,4),
    Balance DECIMAL(18,4),
    Rounding DECIMAL(18,4),
    PaxNumber INT,
    BillDiscountAmount DECIMAL(18,4),
    OrderNo VARCHAR(255),
    LoadedAt DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.staging_sales_items_api (
    ItemID BIGINT,
    SaleID BIGINT,
    ProductID INT,
    ProductCode VARCHAR(255),
    ProductName VARCHAR(255),
    Category VARCHAR(100),
    Quantity DECIMAL(10,2),
    UnitPrice DECIMAL(18,4),
    Subtotal DECIMAL(18,4),
    DiscountAmount DECIMAL(18,4),
    TaxAmount DECIMAL(18,4),
    TaxCode VARCHAR(10),
    TaxRate DECIMAL(5,2),
    Cost DECIMAL(18,4),
    IsFOC BIT,
    Model VARCHAR(100),
    IsServiceCharge BIT,
    SalesType VARCHAR(50),
    SubSalesType VARCHAR(50),
    LoadedAt DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.staging_payments_api (
    PaymentID BIGINT,
    SaleID BIGINT,
    PaymentMethod VARCHAR(50),
    Amount DECIMAL(18,4),
    PaymentDate DATETIME,
    Reference VARCHAR(255),
    EODSessionID VARCHAR(50),
    TenderAmount DECIMAL(18,4),
    ChangeAmount DECIMAL(18,4),
    CardType VARCHAR(100),
    IsVoid BIT,
    LoadedAt DATETIME DEFAULT GETDATE()
);
```

---

## 📊 COMPARISON: API vs Direct DB

### What API Gives You That Direct DB Doesn't

1. ✅ **Clean, structured data** - Xilnex has already done the joins for you
2. ✅ **Official field names** - No guessing which column to use
3. ✅ **Consistent data types** - Always float, str, bool (not mixed)
4. ✅ **SubSalesType field** - Properly populated (vs mostly NULL in DB)
5. ✅ **Complete payment references** - Better traceability
6. ✅ **Vendor-supported** - Won't break with schema changes

### What Direct DB Gives You That API Doesn't

1. ✅ **Date filtering** - Query any date range directly
2. ✅ **SQL flexibility** - Complex joins, aggregations, CTEs
3. ✅ **Instant access** - No pagination, no API rate limits
4. ✅ **Full history** - 7 years of data in seconds
5. ✅ **Custom queries** - Ad-hoc analysis without API limitations

---

## 🎯 FINAL RECOMMENDATION

### Use Both! Hybrid Approach

**Phase 1 (Now - POC):**
- ✅ Keep direct DB ETL for cloud deployment
- ✅ Get POC approved with proven 99.999% accuracy
- ✅ Add new columns to schema (6 critical fields above)

**Phase 2 (Post-POC):**
- ✅ Build API ETL for **daily incremental updates**
- ✅ One-time historical backfill via API (10-20 minutes)
- ✅ Use API for fresh data, DB for historical queries
- ✅ Capture the 6 new fields via API

**Benefits:**
- Fast POC deployment (don't rebuild ETL now)
- Cleaner data going forward (via API)
- Better analytics (new fields captured)
- Future-proof (vendor-supported API)

---

## 📞 Next Steps

1. **Review this mapping** - Confirm which new fields you want
2. **Add schema changes** - Run ALTER TABLE scripts for new columns
3. **Build API ETL (Phase 2)** - After POC approval
4. **Update documentation** - Document new fields in schema guide


