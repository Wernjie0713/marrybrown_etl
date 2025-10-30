<!-- ef70ffde-84db-4802-beee-87180ad5eca6 98fcb034-26a4-4fcb-96dc-830b29e78bd1 -->
# EOD Summary Report - Date Grouping Restructure

## Objective

Change the EOD Summary Report grouping from **Location → Sale Type** to **Date → Location**, with Sale Type as a regular data column. This allows business analysts and outlet managers to analyze trends across dates more effectively.

## Current State

- Grouping: Location → Sale Type → Data
- No date column visible
- Date is only in the filter parameters

## Desired State

- Grouping: Date → Location → Data
- Date column visible in table
- Sale Type as a regular column (multiple rows per location per date)

---

## Implementation Changes

### 1. Backend API Changes

**File:** `marrybrown_api/routers/sales.py`

**Endpoint:** `POST /sales/reports/eod-summary`

**Changes to SQL Query:**

1. Add date to SELECT clause:

   - `d.FullDate as date` (join with dim_date)

2. Add date to GROUP BY clause:

   - `d.FullDate`

3. Add date to ORDER BY clause:

   - Order by `d.FullDate DESC` (newest first), then `l.LocationName`, then `SaleType`

**SQL Query Structure:**

```sql
SELECT 
    d.FullDate as date,
    l.LocationName as location_name,
    f.SaleType as sale_type,
    COUNT(DISTINCT f.SaleNumber) as transaction_count,
    SUM(f.NetAmount) as total_net_amount,
    SUM(f.TaxAmount) as total_tax_amount,
    SUM(f.TotalAmount) as total_gross_amount,
    -- Payment breakdown (conditional aggregation)
    SUM(CASE WHEN pt.PaymentCategory = 'Cash' THEN f.TotalAmount ELSE 0 END) as cash_amount,
    SUM(CASE WHEN pt.PaymentCategory = 'Card' THEN f.TotalAmount ELSE 0 END) as card_amount,
    SUM(CASE WHEN pt.PaymentCategory = 'Voucher' THEN f.TotalAmount ELSE 0 END) as voucher_amount,
    SUM(CASE WHEN pt.PaymentCategory = 'E-Wallet' THEN f.TotalAmount ELSE 0 END) as ewallet_amount,
    SUM(CASE WHEN pt.PaymentCategory = 'Other' THEN f.TotalAmount ELSE 0 END) as other_amount
FROM fact_sales_transactions f
JOIN dim_date d ON f.DateKey = d.DateKey
JOIN dim_locations l ON f.LocationKey = l.LocationKey
LEFT JOIN dim_payment_types pt ON f.PaymentTypeKey = pt.PaymentTypeKey
WHERE d.FullDate BETWEEN :start_date AND :end_date
    AND f.SaleType != 'Return'
    [AND l.LocationKey IN :location_keys] -- if provided
GROUP BY d.FullDate, l.LocationName, f.SaleType
ORDER BY d.FullDate DESC, l.LocationName, f.SaleType
```

---

### 2. Frontend Changes

**File:** `marrybrown-portal/src/pages/reports/EODSummaryReportPage.jsx`

#### Change 1: Update Grouping State (Line 123)

```javascript
// OLD:
const [grouping, setGrouping] = useState(['location_name', 'sale_type']);

// NEW:
const [grouping, setGrouping] = useState(['date', 'location_name']);
```

#### Change 2: Add Date Column (Insert after line 226, before location_name column)

```javascript
{
  accessorKey: 'date',
  size: 120,
  minSize: 100,
  header: ({ column }) => <SortableHeader column={column}>Date</SortableHeader>,
  cell: ({ row }) => {
    if (row.getIsGrouped()) {
      return null; // Group header handled separately
    }
    return (
      <div className="font-medium">{row.getValue('date')}</div>
    );
  },
  getGroupingValue: row => row.date,
},
```

#### Change 3: Update Sale Type Column (Line 242-254)

```javascript
// Remove getGroupingValue and enableGrouping
{
  accessorKey: 'sale_type',
  size: 130,
  minSize: 110,
  header: ({ column }) => <SortableHeader column={column}>Sale Type</SortableHeader>,
  cell: ({ row }) => {
    // Remove the getIsGrouped check - now it's always a data cell
    return <div className="font-medium">{row.getValue('sale_type')}</div>;
  },
  // REMOVE: getGroupingValue: row => row.sale_type,
  enableGrouping: false, // ADD THIS
},
```

#### Change 4: Update getRowId (Line 566)

```javascript
// OLD:
getRowId: (row, index) => `${row.location_name}-${row.sale_type}-${index}`,

// NEW:
getRowId: (row, index) => `${row.date}-${row.location_name}-${row.sale_type}-${index}`,
```

#### Change 5: Update Group Header Rendering (Line 1069-1108)

```javascript
// Update groupLevel determination
const groupLevel = grouping.indexOf(groupingColumnId);
const isDateGroup = groupLevel === 0; // First level is now Date
const isLocationGroup = groupLevel === 1; // Second level is now Location

// Update className:
className={isDateGroup ? "bg-blue-50 hover:bg-blue-100 font-bold" : "bg-green-50 hover:bg-green-100 font-semibold"}

// Update icon and label:
<span className={isDateGroup ? "text-blue-900 text-lg" : "text-green-800"}>
  {isDateGroup ? "📅 " : "🏪 "}
  {groupingColumnId === 'date' ? 'Date: ' : 'Location: '}
  <strong>{groupingValue}</strong>
  <span className="ml-2 text-sm text-gray-600">
    ({row.subRows?.length || 0} {row.subRows?.length === 1 ? 'item' : 'items'})
  </span>
</span>
```

#### Change 6: Update Toggle Grouping Button (Line 942-966)

```javascript
// Update the setGrouping call:
onClick={() => {
  if (grouping.length > 0) {
    setGrouping([]);
    setExpanded({});
  } else {
    setGrouping(['date', 'location_name']); // CHANGED from ['location_name', 'sale_type']
    setExpanded({});
  }
}}
```

#### Change 7: Update Excel Export (Line 676-697)

```javascript
const handleExport = () => {
  const exportData = data.map((row) => ({
    'Date': row.date, // ADD THIS LINE
    'Location': row.location_name,
    'Sale Type': row.sale_type,
    'Transactions': row.transaction_count,
    'Net Amount (RM)': row.total_net_amount,
    'Tax Amount (RM)': row.total_tax_amount,
    'Gross Amount (RM)': row.total_gross_amount,
    'Cash (RM)': row.cash_amount,
    'Card (RM)': row.card_amount,
    'Voucher (RM)': row.voucher_amount,
    'E-Wallet (RM)': row.ewallet_amount,
    'Other (RM)': row.other_amount,
  }));
  // ... rest of the export logic
};
```

#### Change 8: Update Default Sorting (Line 117)

```javascript
// OLD:
const [sorting, setSorting] = useState([{ id: 'location_name', desc: false }]);

// NEW:
const [sorting, setSorting] = useState([{ id: 'date', desc: true }]); // Sort by date descending (newest first)
```

---

## Expected Result

### Visual Structure:

```
📅 Date: 2025-01-15 (5 items)
  └─ 🏪 Location: MB IOI KULAI (3 items)
      ├─ Row: Dine In | 45 txns | RM 2,345.67 | Cash: RM 1,000 | ...
      ├─ Row: Take Away | 30 txns | RM 1,567.89 | Cash: RM 800 | ...
      └─ Row: Delivery | 12 txns | RM 678.90 | Cash: RM 200 | ...
  └─ 🏪 Location: MB AEON TEBRAU (2 items)
      ├─ Row: Dine In | 50 txns | RM 2,890.12 | ...
      └─ Row: Take Away | 35 txns | RM 1,789.45 | ...

📅 Date: 2025-01-14 (4 items)
  └─ ...
```

### Data Flow:

1. User selects date range (e.g., Jan 1-15, 2025)
2. API returns all records grouped by date, location, sale type
3. Frontend groups by Date (primary) → Location (secondary)
4. Each location shows multiple rows (one per sale type)
5. Sub-totals aggregate at both Date and Location levels

---

## Testing Checklist

### Backend Testing:

- [ ] API returns `date` field in response
- [ ] Data is ordered by date DESC, then location, then sale type
- [ ] Date range filter works correctly
- [ ] Location filter works correctly

### Frontend Testing:

- [ ] Date column is visible in the table
- [ ] Grouping shows Date as top level (blue background)
- [ ] Grouping shows Location as second level (green background)
- [ ] Sale Type appears as regular column in data rows
- [ ] Expanding Date group shows locations
- [ ] Expanding Location shows individual sale type rows
- [ ] Sorting by date works (newest first by default)
- [ ] Excel export includes date column
- [ ] JSON export includes date column
- [ ] Row selection works correctly
- [ ] Sub-total aggregations are correct at both levels

### User Experience Testing:

- [ ] Grouped view button toggles between flat and grouped
- [ ] Column visibility toggle includes date column
- [ ] Pagination works with new grouping
- [ ] Mobile responsive design still works
- [ ] No console errors

---

## Files to Modify

1. **Backend:** `marrybrown_api/routers/sales.py` (1 file)
2. **Frontend:** `marrybrown-portal/src/pages/reports/EODSummaryReportPage.jsx` (1 file)

**Total:** 2 files

### To-dos

- [ ] Update EOD summary SQL query to include date field and adjust GROUP BY/ORDER BY
- [ ] Change grouping state from ['location_name', 'sale_type'] to ['date', 'location_name']
- [ ] Add date column to the columns array with proper configuration
- [ ] Convert sale_type from grouping column to regular data column
- [ ] Update getRowId to include date in the unique identifier
- [ ] Update group header rendering for Date (level 0) and Location (level 1)
- [ ] Update Excel and JSON export functions to include date column
- [ ] Update default sorting to sort by date descending (newest first)
- [ ] Test all functionality: grouping, sorting, filtering, exports, and responsiveness