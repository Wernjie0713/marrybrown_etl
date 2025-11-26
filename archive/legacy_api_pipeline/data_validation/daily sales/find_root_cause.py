"""
Find the root cause of ~4x inflation for MB JITRA

Key Finding: DB Total / Xilnex = 3.83x (almost 4x)
Hypothesis: Each line item is being multiplied by # of payment methods

This is the PAYMENT ALLOCATION issue:
- In the ETL, when a sale has multiple payment methods, 
  the TotalAmount is allocated proportionally across payments
- But if the allocation is incorrect, amounts get multiplied
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv('.env.local')

def get_db_engine():
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 17 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER", "localhost")
    database = os.getenv("TARGET_DATABASE", "MarryBrown_DW")
    user = os.getenv("TARGET_USERNAME", "sa")
    password = quote_plus(os.getenv("TARGET_PASSWORD", ""))
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
        "&timeout=60"
    )
    return create_engine(connection_uri, echo=False)

def fmt_money(val):
    if val is None:
        return "RM 0.00"
    return f"RM {float(val):,.2f}"

def find_root_cause():
    engine = get_db_engine()
    
    print("=" * 90)
    print("ROOT CAUSE ANALYSIS: Why is MB JITRA ~4x inflated?")
    print("=" * 90)
    print()
    
    with engine.connect() as conn:
        # ============================================================
        # TEST 1: Check PaymentTypeKey distribution per SaleNumber
        # ============================================================
        print("TEST 1: How many PaymentTypeKeys per SaleNumber?")
        print("-" * 90)
        print("   If each sale has ~4 payment types, that could explain 4x inflation")
        print()
        
        query1 = text("""
            SELECT 
                f.SaleNumber,
                COUNT(DISTINCT f.PaymentTypeKey) as distinct_payment_types,
                COUNT(*) as total_rows,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber
            ORDER BY COUNT(DISTINCT f.PaymentTypeKey) DESC
        """)
        result1 = conn.execute(query1).fetchall()
        
        # Distribution of payment type counts
        payment_type_dist = {}
        for row in result1:
            pt_count = row.distinct_payment_types
            if pt_count not in payment_type_dist:
                payment_type_dist[pt_count] = 0
            payment_type_dist[pt_count] += 1
        
        print(f"   Distribution of payment types per sale:")
        for pt_count in sorted(payment_type_dist.keys()):
            count = payment_type_dist[pt_count]
            print(f"   - {pt_count} payment type(s): {count} sales")
        print()
        
        # ============================================================
        # TEST 2: Check a specific SaleNumber with multiple payment types
        # ============================================================
        print("TEST 2: Detailed look at sales with multiple payment types")
        print("-" * 90)
        
        # Find sales with >1 payment type
        multi_payment_sales = [row for row in result1 if row.distinct_payment_types > 1]
        
        if multi_payment_sales:
            sample_sale = multi_payment_sales[0].SaleNumber
            print(f"   Examining SaleNumber: {sample_sale}")
            print()
            
            query2 = text("""
                SELECT 
                    f.SaleNumber,
                    f.ProductKey,
                    p.ProductName,
                    pt.PaymentMethodName,
                    f.TotalAmount,
                    f.NetAmount,
                    f.Quantity
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                LEFT JOIN dbo.dim_products p ON f.ProductKey = p.ProductKey
                LEFT JOIN dbo.dim_payment_types pt ON f.PaymentTypeKey = pt.PaymentTypeKey
                WHERE l.LocationName = 'MB JITRA'
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleNumber = :sale_number
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
                ORDER BY p.ProductName, pt.PaymentMethodName
            """)
            result2 = conn.execute(query2, {"sale_number": sample_sale}).fetchall()
            
            print(f"   {'ProductName':<30} {'PaymentMethod':<15} {'TotalAmount':>12} {'NetAmount':>12} {'Qty':>6}")
            print("   " + "-" * 75)
            for row in result2:
                product = (row.ProductName or "N/A")[:30]
                payment = (row.PaymentMethodName or "N/A")[:15]
                print(f"   {product:<30} {payment:<15} {fmt_money(row.TotalAmount):>12} {fmt_money(row.NetAmount):>12} {row.Quantity or 0:>6.0f}")
            
            # Sum up
            total = sum(float(row.TotalAmount or 0) for row in result2)
            print("   " + "-" * 75)
            print(f"   {'SUM':<45} {fmt_money(total):>12}")
            print()
        else:
            print("   No sales with multiple payment types found")
        print()
        
        # ============================================================
        # TEST 3: Compare with a store that matches Xilnex
        # ============================================================
        print("TEST 3: Compare with MB CENTRAL I-CITY (which matches Xilnex)")
        print("-" * 90)
        
        query3 = text("""
            SELECT 
                f.SaleNumber,
                COUNT(DISTINCT f.PaymentTypeKey) as distinct_payment_types,
                COUNT(DISTINCT f.ProductKey) as distinct_products,
                COUNT(*) as total_rows,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB CENTRAL I-CITY'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber
        """)
        result3 = conn.execute(query3).fetchall()
        
        # Distribution
        pt_dist = {}
        for row in result3:
            pt_count = row.distinct_payment_types
            if pt_count not in pt_dist:
                pt_dist[pt_count] = 0
            pt_dist[pt_count] += 1
        
        print(f"   Distribution of payment types per sale (MB CENTRAL I-CITY):")
        for pt_count in sorted(pt_dist.keys()):
            count = pt_dist[pt_count]
            print(f"   - {pt_count} payment type(s): {count} sales")
        
        # Calculate avg rows per sale
        total_sales = len(result3)
        total_rows = sum(row.total_rows for row in result3)
        avg_rows = total_rows / total_sales if total_sales > 0 else 0
        
        print(f"\n   Total Sales: {total_sales}")
        print(f"   Total Rows: {total_rows}")
        print(f"   Avg Rows per Sale: {avg_rows:.2f}")
        print()
        
        # Same for MB JITRA
        query3b = text("""
            SELECT 
                f.SaleNumber,
                COUNT(DISTINCT f.PaymentTypeKey) as distinct_payment_types,
                COUNT(DISTINCT f.ProductKey) as distinct_products,
                COUNT(*) as total_rows,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber
        """)
        result3b = conn.execute(query3b).fetchall()
        
        total_sales_j = len(result3b)
        total_rows_j = sum(row.total_rows for row in result3b)
        avg_rows_j = total_rows_j / total_sales_j if total_sales_j > 0 else 0
        
        print(f"   MB JITRA:")
        print(f"   Total Sales: {total_sales_j}")
        print(f"   Total Rows: {total_rows_j}")
        print(f"   Avg Rows per Sale: {avg_rows_j:.2f}")
        print()
        
        # ============================================================
        # TEST 4: Check if ProductKey is duplicated per PaymentTypeKey
        # ============================================================
        print("TEST 4: Check if same ProductKey appears multiple times per SaleNumber")
        print("-" * 90)
        print("   This would indicate line-item duplication across payment types")
        print()
        
        query4 = text("""
            SELECT 
                f.SaleNumber,
                f.ProductKey,
                COUNT(*) as occurrence_count,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber, f.ProductKey
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """)
        result4 = conn.execute(query4).fetchall()
        
        if result4:
            print(f"   [FOUND] {len(result4)} cases where same ProductKey appears multiple times in same sale!")
            print()
            print(f"   {'SaleNumber':<15} {'ProductKey':>12} {'Occurrences':>12} {'TotalAmount':>15}")
            print("   " + "-" * 54)
            for row in result4[:20]:
                print(f"   {row.SaleNumber:<15} {row.ProductKey:>12} {row.occurrence_count:>12} {fmt_money(row.total_amount):>15}")
            if len(result4) > 20:
                print(f"   ... and {len(result4) - 20} more")
        else:
            print("   [OK] No duplicate ProductKey per SaleNumber found")
        print()
        
        # ============================================================
        # TEST 5: CRITICAL - Check the actual grain of the fact table
        # ============================================================
        print("TEST 5: What is the ACTUAL grain of fact_sales_transactions?")
        print("-" * 90)
        print("   Expected grain: One row per SaleNumber + ProductKey + PaymentTypeKey")
        print("   If grain is different, totals will be wrong")
        print()
        
        # Check if SaleNumber + ProductKey + PaymentTypeKey is unique
        query5 = text("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT CONCAT(f.SaleNumber, '-', f.ProductKey, '-', f.PaymentTypeKey)) as distinct_grain
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        result5 = conn.execute(query5).first()
        
        print(f"   Total Rows: {result5.total_rows}")
        print(f"   Distinct (SaleNumber + ProductKey + PaymentTypeKey): {result5.distinct_grain}")
        
        if result5.total_rows == result5.distinct_grain:
            print("   [OK] Grain is correct - one row per SaleNumber + ProductKey + PaymentTypeKey")
        else:
            dup_count = result5.total_rows - result5.distinct_grain
            print(f"   [WARNING] {dup_count} duplicate rows at this grain level!")
        print()
        
        # ============================================================
        # TEST 6: What should the SUM be if we de-duplicate by sale header?
        # ============================================================
        print("TEST 6: Calculate what SUM should be at SALE HEADER level")
        print("-" * 90)
        print("   If Xilnex sums at sale header level (not line items), what's the total?")
        print()
        
        # Get the "header total" for each sale (this would be the TotalAmount from the sale header, not sum of lines)
        # Since we don't have a separate header table, we need to figure out the logic
        
        # Hypothesis: The correct total per sale is (sum of line items) / (number of payment types)
        # Because each line item is repeated for each payment type
        
        query6 = text("""
            SELECT 
                SaleNumber,
                SUM(TotalAmount) as line_sum,
                COUNT(DISTINCT PaymentTypeKey) as payment_types,
                SUM(TotalAmount) / NULLIF(COUNT(DISTINCT PaymentTypeKey), 0) as adjusted_total
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY SaleNumber
        """)
        result6 = conn.execute(query6).fetchall()
        
        # Sum the adjusted totals
        adjusted_grand_total = sum(float(row.adjusted_total or 0) for row in result6)
        original_grand_total = sum(float(row.line_sum or 0) for row in result6)
        
        print(f"   Original Grand Total (sum of all lines): {fmt_money(original_grand_total)}")
        print(f"   Adjusted Grand Total (line_sum / payment_types): {fmt_money(adjusted_grand_total)}")
        print(f"   Xilnex Target: RM 5,999.35")
        print()
        
        if abs(adjusted_grand_total - 5999.35) < 100:
            print("   [MATCH!] Adjusted total is close to Xilnex!")
            print("   This confirms: line items are duplicated per payment type,")
            print("   and we need to divide by the number of payment types.")
        else:
            print(f"   [NO MATCH] Adjusted total ({fmt_money(adjusted_grand_total)}) doesn't match Xilnex")
        print()
        
        # ============================================================
        # SUMMARY
        # ============================================================
        print("=" * 90)
        print("ROOT CAUSE SUMMARY")
        print("=" * 90)
        
        # Calculate the ratio
        if original_grand_total > 0:
            ratio = original_grand_total / 5999.35
            print(f"""
    FINDINGS:
    
    1. Database Total: {fmt_money(original_grand_total)}
    2. Xilnex Total:   RM 5,999.35
    3. Ratio:          {ratio:.2f}x
    
    4. Adjusted Total (divided by payment types): {fmt_money(adjusted_grand_total)}
    
    ROOT CAUSE HYPOTHESIS:
    
    The ETL is creating multiple rows for each line item - one per payment type.
    When we SUM(TotalAmount), we're counting each item multiple times.
    
    For example:
    - Sale #123 has 4 items and 2 payment methods (cash + card)
    - ETL creates 8 rows (4 items x 2 payment types)
    - Each row has the FULL item TotalAmount (not proportionally allocated)
    - SUM(TotalAmount) = 2x the actual sale total
    
    FIX OPTIONS:
    
    A) QUERY FIX (immediate):
       - Divide by COUNT(DISTINCT PaymentTypeKey) in the API query
       - Or use SUM(TotalAmount) / COUNT(DISTINCT PaymentTypeKey) per sale
    
    B) ETL FIX (better, requires re-run):
       - Allocate TotalAmount proportionally across payment types
       - Or don't duplicate line items per payment type
    
    C) VALIDATION:
       - Test with the adjusted formula and compare with Xilnex
            """)

if __name__ == "__main__":
    find_root_cause()

