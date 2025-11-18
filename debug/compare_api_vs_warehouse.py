"""
API vs Warehouse Comparison Script
Compares Xilnex Sync API data with current warehouse data for validation

Author: YONG WERN JIE
Date: October 28, 2025
Purpose: Phase 1 validation - Determine if API data matches warehouse/portal
"""

import http.client
import json
import os
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

# Xilnex API Credentials
API_HOST = "api.xilnex.com"
APP_ID = "OzHIEJmCKqq8fPZkgZoogJeRgOsVhzAE"
TOKEN = "v5_29jNAaSZnVmPq6idx8LBQ/Vw01qxfqrRrNpgF6uV6fE="
AUTH_LEVEL = "5"

# Warehouse Connection (from environment variables - same as your ETL scripts)
def get_warehouse_engine():
    """Get SQLAlchemy engine for warehouse (same as transform scripts)"""
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER", "localhost")
    database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
    user = os.getenv("TARGET_USERNAME", "sa")
    password = os.getenv("TARGET_PASSWORD", "")
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    
    return create_engine(connection_uri, pool_pre_ping=True)

# Test Parameters
TEST_OUTLET = "A FAMOSA"  # Filter by this outlet
TEST_START_DATE = "2025-09-01"  # September 2025 start
TEST_END_DATE = "2025-09-30"    # September 2025 end

# ============================================================================
# API DATA EXTRACTION
# ============================================================================

def call_sync_api(start_timestamp=None, max_calls=50):
    """
    Call Xilnex Sync API to retrieve sales data
    
    Args:
        start_timestamp: Optional timestamp for incremental sync
        max_calls: Maximum number of API calls (safety limit)
    
    Returns:
        List of all sales records
    """
    print("\n" + "="*80)
    print("EXTRACTING DATA FROM XILNEX SYNC API")
    print("="*80)
    
    all_sales = []
    current_timestamp = start_timestamp
    call_count = 0
    
    headers = {
        'Accept': 'application/json, text/json, text/html, application/*+json',
        'Content-Type': 'application/json',
        'appid': APP_ID,
        'token': TOKEN,
        'auth': AUTH_LEVEL,
    }
    
    while call_count < max_calls:
        call_count += 1
        
        # Build URL
        url_path = "/apps/v2/sync/sales"
        if current_timestamp:
            url_path += f"?starttimestamp={current_timestamp}"
        
        print(f"\n[Call {call_count}] Fetching batch...")
        print(f"  URL: {url_path}")
        
        try:
            # Make API call
            conn = http.client.HTTPSConnection(API_HOST, timeout=30)
            conn.request("GET", url_path, headers=headers)
            res = conn.getresponse()
            
            if res.status != 200:
                print(f"  ERROR: HTTP {res.status}")
                print(f"  Response: {res.read().decode('utf-8')[:500]}")
                break
            
            # Parse response
            data = json.loads(res.read().decode("utf-8"))
            
            if not data.get('ok'):
                print(f"  ERROR: API returned ok=false")
                break
            
            sales_batch = data.get('data', {}).get('sales', [])
            
            if not sales_batch:
                print(f"  No more sales records. Stopping.")
                break
            
            # Filter by date range and outlet
            filtered_sales = []
            for sale in sales_batch:
                # Check business date
                business_date = sale.get('businessDateTime', '').split('T')[0]
                if business_date < TEST_START_DATE or business_date > TEST_END_DATE:
                    continue
                
                # Check outlet
                outlet = sale.get('outlet', '')
                if TEST_OUTLET.upper() not in outlet.upper():
                    continue
                
                filtered_sales.append(sale)
            
            all_sales.extend(filtered_sales)
            
            print(f"  Retrieved: {len(sales_batch)} sales")
            print(f"  Filtered (date + outlet): {len(filtered_sales)} sales")
            print(f"  Total so far: {len(all_sales)} sales")
            
            # Get next timestamp
            current_timestamp = data.get('data', {}).get('lastTimestamp')
            if not current_timestamp:
                print(f"  No more timestamps. Stopping.")
                break
            
            print(f"  Next timestamp: {current_timestamp}")
            
            # Check if we've passed the end date
            if sales_batch and sales_batch[-1].get('businessDateTime', '').split('T')[0] > TEST_END_DATE:
                print(f"  Passed end date. Stopping.")
                break
                
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            break
        finally:
            conn.close()
    
    print(f"\n[COMPLETE] Total sales retrieved: {len(all_sales)}")
    return all_sales


def analyze_api_data(sales):
    """Analyze API sales data and extract key metrics"""
    print("\n" + "="*80)
    print("ANALYZING API DATA")
    print("="*80)
    
    metrics = {
        'total_sales': len(sales),
        'total_grand_total': 0.0,
        'total_net_amount': 0.0,
        'total_tax': 0.0,
        'total_items': 0,
        'total_payments': 0,
        'payment_methods': defaultdict(float),
        'sales_types': defaultdict(int),
        'unique_dates': set(),
        'unique_outlets': set(),
    }
    
    for sale in sales:
        # Sale level metrics
        metrics['total_grand_total'] += sale.get('grandTotal', 0.0) or 0.0
        metrics['total_net_amount'] += sale.get('netAmount', 0.0) or 0.0
        metrics['total_tax'] += sale.get('gstTaxAmount', 0.0) or 0.0
        
        # Items
        items = sale.get('items', [])
        metrics['total_items'] += len(items)
        
        # Payments
        payments = sale.get('collection', [])
        metrics['total_payments'] += len(payments)
        for payment in payments:
            method = payment.get('method', 'Unknown')
            amount = payment.get('amount', 0.0) or 0.0
            metrics['payment_methods'][method] += amount
        
        # Sales types
        sales_type = sale.get('salesType', 'Unknown')
        metrics['sales_types'][sales_type] += 1
        
        # Dates and outlets
        business_date = sale.get('businessDateTime', '').split('T')[0]
        if business_date:
            metrics['unique_dates'].add(business_date)
        outlet = sale.get('outlet', 'Unknown')
        metrics['unique_outlets'].add(outlet)
    
    # Print summary
    print(f"\nSales Summary:")
    print(f"  Total Sales: {metrics['total_sales']:,}")
    print(f"  Total Items: {metrics['total_items']:,}")
    print(f"  Total Payments: {metrics['total_payments']:,}")
    print(f"\nFinancial Summary:")
    print(f"  Grand Total: RM {metrics['total_grand_total']:,.2f}")
    print(f"  Net Amount:  RM {metrics['total_net_amount']:,.2f}")
    print(f"  Total Tax:   RM {metrics['total_tax']:,.2f}")
    print(f"\nPayment Methods:")
    for method, amount in sorted(metrics['payment_methods'].items()):
        print(f"  {method}: RM {amount:,.2f}")
    print(f"\nSales Types:")
    for sales_type, count in sorted(metrics['sales_types'].items()):
        print(f"  {sales_type}: {count:,} sales")
    print(f"\nDate Range:")
    if metrics['unique_dates']:
        dates = sorted(metrics['unique_dates'])
        print(f"  From: {dates[0]}")
        print(f"  To:   {dates[-1]}")
        print(f"  Days: {len(dates)}")
    print(f"\nOutlets:")
    for outlet in sorted(metrics['unique_outlets']):
        print(f"  {outlet}")
    
    return metrics


# ============================================================================
# WAREHOUSE DATA EXTRACTION
# ============================================================================

def query_warehouse():
    """Query warehouse for same date range and outlet"""
    print("\n" + "="*80)
    print("EXTRACTING DATA FROM WAREHOUSE")
    print("="*80)
    
    try:
        # Get warehouse engine
        engine = get_warehouse_engine()
        
        print(f"\nConnecting to warehouse...")
        conn = engine.connect()
        
        # Query sales summary
        query = """
        SELECT 
            COUNT(DISTINCT fs.SalesTransactionID) as total_sales,
            COUNT(DISTINCT fsi.ItemID) as total_items,
            COUNT(DISTINCT fp.PaymentID) as total_payments,
            SUM(fs.GrandTotal) as total_grand_total,
            SUM(fs.NetAmount) as total_net_amount,
            SUM(fs.TaxAmount) as total_tax
        FROM dbo.fact_sales_transactions fs
        LEFT JOIN dbo.fact_sales_items fsi ON fs.SalesTransactionID = fsi.SalesTransactionID
        LEFT JOIN dbo.fact_payments fp ON fs.SalesTransactionID = fp.SalesTransactionID
        INNER JOIN dbo.dim_locations dl ON fs.LocationID = dl.LocationID
        INNER JOIN dbo.dim_date dd ON fs.DateID = dd.DateID
        WHERE dd.Date BETWEEN :start_date AND :end_date
          AND dl.LocationName LIKE :outlet
        """
        
        print(f"\nQuerying warehouse...")
        print(f"  Date Range: {TEST_START_DATE} to {TEST_END_DATE}")
        print(f"  Outlet: {TEST_OUTLET}")
        
        result = conn.execute(text(query), {
            'start_date': TEST_START_DATE,
            'end_date': TEST_END_DATE,
            'outlet': f'%{TEST_OUTLET}%'
        })
        row = result.fetchone()
        
        metrics = {
            'total_sales': row[0] or 0,
            'total_items': row[1] or 0,
            'total_payments': row[2] or 0,
            'total_grand_total': float(row[3] or 0),
            'total_net_amount': float(row[4] or 0),
            'total_tax': float(row[5] or 0),
        }
        
        # Query payment methods
        payment_query = """
        SELECT 
            dpt.PaymentType,
            SUM(fp.Amount) as total_amount
        FROM dbo.fact_payments fp
        INNER JOIN dbo.dim_payment_types dpt ON fp.PaymentTypeID = dpt.PaymentTypeID
        INNER JOIN dbo.fact_sales_transactions fs ON fp.SalesTransactionID = fs.SalesTransactionID
        INNER JOIN dbo.dim_locations dl ON fs.LocationID = dl.LocationID
        INNER JOIN dbo.dim_date dd ON fs.DateID = dd.DateID
        WHERE dd.Date BETWEEN :start_date AND :end_date
          AND dl.LocationName LIKE :outlet
        GROUP BY dpt.PaymentType
        """
        
        result = conn.execute(text(payment_query), {
            'start_date': TEST_START_DATE,
            'end_date': TEST_END_DATE,
            'outlet': f'%{TEST_OUTLET}%'
        })
        metrics['payment_methods'] = {}
        for row in result.fetchall():
            metrics['payment_methods'][row[0]] = float(row[1])
        
        # Query sales types
        sales_type_query = """
        SELECT 
            fs.SalesType,
            COUNT(*) as count
        FROM dbo.fact_sales_transactions fs
        INNER JOIN dbo.dim_locations dl ON fs.LocationID = dl.LocationID
        INNER JOIN dbo.dim_date dd ON fs.DateID = dd.DateID
        WHERE dd.Date BETWEEN :start_date AND :end_date
          AND dl.LocationName LIKE :outlet
        GROUP BY fs.SalesType
        """
        
        result = conn.execute(text(sales_type_query), {
            'start_date': TEST_START_DATE,
            'end_date': TEST_END_DATE,
            'outlet': f'%{TEST_OUTLET}%'
        })
        metrics['sales_types'] = {}
        for row in result.fetchall():
            metrics['sales_types'][row[0]] = row[1]
        
        conn.close()
        
        # Print summary
        print(f"\nWarehouse Summary:")
        print(f"  Total Sales: {metrics['total_sales']:,}")
        print(f"  Total Items: {metrics['total_items']:,}")
        print(f"  Total Payments: {metrics['total_payments']:,}")
        print(f"\nFinancial Summary:")
        print(f"  Grand Total: RM {metrics['total_grand_total']:,.2f}")
        print(f"  Net Amount:  RM {metrics['total_net_amount']:,.2f}")
        print(f"  Total Tax:   RM {metrics['total_tax']:,.2f}")
        print(f"\nPayment Methods:")
        for method, amount in sorted(metrics['payment_methods'].items()):
            print(f"  {method}: RM {amount:,.2f}")
        print(f"\nSales Types:")
        for sales_type, count in sorted(metrics['sales_types'].items()):
            print(f"  {sales_type}: {count:,} sales")
        
        return metrics
        
    except Exception as e:
        print(f"\nERROR querying warehouse: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# COMPARISON
# ============================================================================

def compare_metrics(api_metrics, warehouse_metrics):
    """Compare API metrics with warehouse metrics"""
    print("\n" + "="*80)
    print("COMPARISON RESULTS")
    print("="*80)
    
    if not warehouse_metrics:
        print("\n⚠️  Warehouse data not available. Skipping comparison.")
        return
    
    # Comparison table
    comparisons = [
        ('Total Sales Count', api_metrics['total_sales'], warehouse_metrics['total_sales']),
        ('Total Items Count', api_metrics['total_items'], warehouse_metrics['total_items']),
        ('Total Payments Count', api_metrics['total_payments'], warehouse_metrics['total_payments']),
        ('Grand Total (RM)', api_metrics['total_grand_total'], warehouse_metrics['total_grand_total']),
        ('Net Amount (RM)', api_metrics['total_net_amount'], warehouse_metrics['total_net_amount']),
        ('Total Tax (RM)', api_metrics['total_tax'], warehouse_metrics['total_tax']),
    ]
    
    print(f"\n{'Metric':<25} {'API':>20} {'Warehouse':>20} {'Match':>10} {'Diff %':>10}")
    print("-" * 90)
    
    all_match = True
    for metric, api_val, wh_val in comparisons:
        # Calculate difference
        if wh_val == 0:
            diff_pct = 0.0 if api_val == 0 else 100.0
        else:
            diff_pct = abs((api_val - wh_val) / wh_val * 100)
        
        # Determine match status (allow 0.01% tolerance for floating point)
        match = "✅" if diff_pct < 0.01 else "❌"
        if diff_pct >= 0.01:
            all_match = False
        
        # Format values
        if 'RM' in metric:
            api_str = f"RM {api_val:,.2f}"
            wh_str = f"RM {wh_val:,.2f}"
        else:
            api_str = f"{api_val:,}"
            wh_str = f"{wh_val:,}"
        
        print(f"{metric:<25} {api_str:>20} {wh_str:>20} {match:>10} {diff_pct:>9.2f}%")
    
    # Payment methods comparison
    print(f"\n{'Payment Method':<25} {'API':>20} {'Warehouse':>20} {'Match':>10}")
    print("-" * 80)
    
    all_payment_methods = set(api_metrics['payment_methods'].keys()) | set(warehouse_metrics.get('payment_methods', {}).keys())
    for method in sorted(all_payment_methods):
        api_amt = api_metrics['payment_methods'].get(method, 0.0)
        wh_amt = warehouse_metrics.get('payment_methods', {}).get(method, 0.0)
        
        if wh_amt == 0:
            diff_pct = 0.0 if api_amt == 0 else 100.0
        else:
            diff_pct = abs((api_amt - wh_amt) / wh_amt * 100)
        
        match = "✅" if diff_pct < 0.01 else "❌"
        
        print(f"{method:<25} RM {api_amt:>17,.2f} RM {wh_amt:>17,.2f} {match:>10}")
    
    # Overall result
    print("\n" + "="*80)
    if all_match:
        print("✅ PERFECT MATCH! API data matches warehouse data 100%")
        print("\nConclusion: Both API and Direct DB approaches produce identical results.")
        print("Recommendation: Choose based on ease of maintenance (API is simpler).")
    else:
        print("❌ DIFFERENCES FOUND between API and warehouse data")
        print("\nNext Steps:")
        print("1. Export same date range from Xilnex Portal")
        print("2. Compare portal export with API and warehouse")
        print("3. Identify which source matches portal (that's your source of truth)")
    print("="*80)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("API VS WAREHOUSE COMPARISON - PHASE 1 VALIDATION")
    print("="*80)
    print(f"\nTest Parameters:")
    print(f"  Date Range: {TEST_START_DATE} to {TEST_END_DATE}")
    print(f"  Outlet: {TEST_OUTLET}")
    print(f"  Purpose: Determine if API or Direct DB is more accurate")
    
    # Step 1: Extract API data
    api_sales = call_sync_api()
    
    if not api_sales:
        print("\n❌ No API data retrieved. Cannot proceed with comparison.")
        return
    
    # Save API data for reference
    api_file = f"api_comparison_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(api_file, 'w', encoding='utf-8') as f:
        json.dump({'sales': api_sales, 'count': len(api_sales)}, f, indent=2, default=str)
    print(f"\n[SAVED] API data saved to: {api_file}")
    
    # Step 2: Analyze API data
    api_metrics = analyze_api_data(api_sales)
    
    # Step 3: Query warehouse
    warehouse_metrics = query_warehouse()
    
    # Step 4: Compare
    compare_metrics(api_metrics, warehouse_metrics)
    
    print("\n" + "="*80)
    print("NEXT STEPS:")
    print("="*80)
    print("1. Export same date range (Sep 2025, A FAMOSA) from Xilnex Portal")
    print("2. Save as: xilnex_portal_export_sep2025_afamosa.xlsx")
    print("3. Compare totals manually:")
    print("   - If API matches portal better → Use API for ETL")
    print("   - If Warehouse matches portal better → Keep direct DB")
    print("   - If both match equally → Choose API (easier maintenance)")
    print("="*80)


if __name__ == "__main__":
    main()

