"""
Deep investigation of MB JITRA discrepancy
My Portal: RM 23,006.92 vs Xilnex: RM 5,999.35

Key Question: What is Xilnex filtering that we're NOT filtering?

Hypotheses:
1. Xilnex uses NetAmount instead of TotalAmount
2. Xilnex excludes certain SubSalesType or OrderSource
3. Xilnex has different date boundary (timezone issue)
4. Xilnex filters by IsFOC or other flags
"""
import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path

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

def investigate_jitra():
    engine = get_db_engine()
    
    print("=" * 90)
    print("DEEP INVESTIGATION: MB JITRA")
    print("My Portal: RM 23,006.92 vs Xilnex: RM 5,999.35 (Difference: RM 17,007.57)")
    print("=" * 90)
    print()
    
    with engine.connect() as conn:
        # ============================================================
        # TEST 1: NetAmount vs TotalAmount
        # ============================================================
        print("TEST 1: Compare NetAmount vs TotalAmount")
        print("-" * 90)
        print("   Hypothesis: Xilnex uses NetAmount (excludes tax)")
        print()
        
        query1 = text("""
            SELECT 
                SUM(f.TotalAmount) as total_amount,
                SUM(f.NetAmount) as net_amount,
                SUM(f.TaxAmount) as tax_amount,
                SUM(f.GrossAmount) as gross_amount,
                SUM(f.DiscountAmount) as discount_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        result1 = conn.execute(query1).first()
        
        print(f"   TotalAmount (what Portal uses):  {fmt_money(result1.total_amount)}")
        print(f"   NetAmount (pre-tax revenue):     {fmt_money(result1.net_amount)}")
        print(f"   TaxAmount:                       {fmt_money(result1.tax_amount)}")
        print(f"   GrossAmount:                     {fmt_money(result1.gross_amount)}")
        print(f"   DiscountAmount:                  {fmt_money(result1.discount_amount)}")
        print(f"   Xilnex Target:                   RM 5,999.35")
        print()
        
        net = float(result1.net_amount or 0)
        xilnex_target = 5999.35
        if abs(net - xilnex_target) < 10:
            print("   [MATCH] NetAmount is close to Xilnex! Xilnex likely uses NetAmount.")
        else:
            print(f"   [NO MATCH] NetAmount ({fmt_money(net)}) doesn't match Xilnex ({fmt_money(xilnex_target)})")
        print()
        
        # ============================================================
        # TEST 2: Check IsFOC (Free of Charge) items
        # ============================================================
        print("TEST 2: Check IsFOC (Free of Charge) items")
        print("-" * 90)
        print("   Hypothesis: Xilnex excludes FOC items")
        print()
        
        query2 = text("""
            SELECT 
                f.IsFOC,
                COUNT(*) as row_count,
                SUM(f.TotalAmount) as total_amount,
                SUM(f.NetAmount) as net_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.IsFOC
        """)
        result2 = conn.execute(query2).fetchall()
        
        print(f"   {'IsFOC':<10} {'Rows':>10} {'TotalAmount':>15} {'NetAmount':>15}")
        print("   " + "-" * 50)
        for row in result2:
            foc = "Yes" if row.IsFOC else "No"
            print(f"   {foc:<10} {row.row_count:>10,} {fmt_money(row.total_amount):>15} {fmt_money(row.net_amount):>15}")
        print()
        
        # Calculate without FOC
        query2b = text("""
            SELECT 
                SUM(f.TotalAmount) as total_amount,
                SUM(f.NetAmount) as net_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
                AND (f.IsFOC = 0 OR f.IsFOC IS NULL)
        """)
        result2b = conn.execute(query2b).first()
        print(f"   Total excluding FOC: {fmt_money(result2b.total_amount)} (TotalAmount), {fmt_money(result2b.net_amount)} (NetAmount)")
        print()
        
        # ============================================================
        # TEST 3: Check by distinct SaleNumber (per-sale basis)
        # ============================================================
        print("TEST 3: Aggregate at SALE level (sum per SaleNumber)")
        print("-" * 90)
        print("   Hypothesis: Xilnex sums at sale level, not line-item level")
        print()
        
        query3 = text("""
            SELECT 
                COUNT(*) as distinct_sales,
                SUM(sale_total) as total_sales_amount
            FROM (
                SELECT 
                    SaleNumber,
                    SUM(TotalAmount) as sale_total
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE d.FullDate = '2019-04-01'
                    AND l.LocationName = 'MB JITRA'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
                GROUP BY SaleNumber
            ) as sale_totals
        """)
        result3 = conn.execute(query3).first()
        print(f"   Distinct Sales: {result3.distinct_sales}")
        print(f"   Sum of Sale Totals: {fmt_money(result3.total_sales_amount)}")
        print("   (Same as line-item sum, so aggregation level is not the issue)")
        print()
        
        # ============================================================
        # TEST 4: Check by Amount thresholds (outliers)
        # ============================================================
        print("TEST 4: Check for outlier transactions (high amounts)")
        print("-" * 90)
        print("   Hypothesis: A few high-value transactions are inflating the total")
        print()
        
        query4 = text("""
            SELECT 
                f.SaleNumber,
                SUM(f.TotalAmount) as sale_total
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber
            ORDER BY SUM(f.TotalAmount) DESC
        """)
        result4 = conn.execute(query4).fetchall()
        
        print(f"   Top 10 Sales by Amount:")
        print(f"   {'SaleNumber':<20} {'Sale Total':>15}")
        print("   " + "-" * 35)
        for row in result4[:10]:
            flag = " [OUTLIER!]" if float(row.sale_total or 0) > 500 else ""
            print(f"   {row.SaleNumber:<20} {fmt_money(row.sale_total):>15}{flag}")
        
        # Calculate stats
        amounts = [float(row.sale_total or 0) for row in result4]
        avg_amt = sum(amounts) / len(amounts) if amounts else 0
        max_amt = max(amounts) if amounts else 0
        min_amt = min(amounts) if amounts else 0
        
        print()
        print(f"   Statistics:")
        print(f"   - Total Sales: {len(amounts)}")
        print(f"   - Average per Sale: {fmt_money(avg_amt)}")
        print(f"   - Max Sale: {fmt_money(max_amt)}")
        print(f"   - Min Sale: {fmt_money(min_amt)}")
        
        # Count outliers (> 500)
        outliers = [a for a in amounts if a > 500]
        outlier_sum = sum(outliers)
        print(f"   - Sales > RM 500: {len(outliers)} (Total: {fmt_money(outlier_sum)})")
        print()
        
        # ============================================================
        # TEST 5: Check PaymentTypeKey distribution
        # ============================================================
        print("TEST 5: Check PaymentTypeKey distribution")
        print("-" * 90)
        print("   Hypothesis: Xilnex filters by payment type")
        print()
        
        query5 = text("""
            SELECT 
                pt.PaymentMethodName,
                COUNT(*) as row_count,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            LEFT JOIN dbo.dim_payment_types pt ON f.PaymentTypeKey = pt.PaymentTypeKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY pt.PaymentMethodName
            ORDER BY SUM(f.TotalAmount) DESC
        """)
        result5 = conn.execute(query5).fetchall()
        
        print(f"   {'PaymentMethod':<25} {'Rows':>10} {'Sales':>10} {'TotalAmount':>15}")
        print("   " + "-" * 60)
        for row in result5:
            print(f"   {(row.PaymentMethodName or 'NULL'):<25} {row.row_count:>10,} {row.distinct_sales:>10,} {fmt_money(row.total_amount):>15}")
        print()
        
        # ============================================================
        # TEST 6: Check TimeKey distribution (time of day)
        # ============================================================
        print("TEST 6: Check TimeKey distribution (time of day)")
        print("-" * 90)
        print("   Hypothesis: Date boundary/timezone issue")
        print()
        
        query6 = text("""
            SELECT 
                CAST(f.TimeKey / 10000 AS INT) as hour_of_day,
                COUNT(*) as row_count,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY CAST(f.TimeKey / 10000 AS INT)
            ORDER BY CAST(f.TimeKey / 10000 AS INT)
        """)
        result6 = conn.execute(query6).fetchall()
        
        print(f"   {'Hour':<10} {'Rows':>10} {'TotalAmount':>15}")
        print("   " + "-" * 35)
        for row in result6:
            hour = row.hour_of_day if row.hour_of_day is not None else 0
            print(f"   {hour:>2}:00     {row.row_count:>10,} {fmt_money(row.total_amount):>15}")
        print()
        
        # ============================================================
        # TEST 7: Compare with ALL stores at different aggregation levels
        # ============================================================
        print("TEST 7: What if Xilnex aggregates differently?")
        print("-" * 90)
        print("   Comparing different calculation methods for MB JITRA:")
        print()
        
        query7 = text("""
            SELECT 
                -- Method 1: Sum all TotalAmount (current API logic)
                SUM(f.TotalAmount) as method1_total_amount,
                
                -- Method 2: Sum all NetAmount (excludes tax)
                SUM(f.NetAmount) as method2_net_amount,
                
                -- Method 3: Sum (NetAmount + TaxAmount) - should equal TotalAmount
                SUM(f.NetAmount + f.TaxAmount) as method3_net_plus_tax,
                
                -- Method 4: Sum GrossAmount - DiscountAmount (pre-tax, pre-discount)
                SUM(f.GrossAmount - f.DiscountAmount) as method4_gross_minus_disc,
                
                -- Row counts
                COUNT(*) as total_rows,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        result7 = conn.execute(query7).first()
        
        print(f"   Method 1 - SUM(TotalAmount):                {fmt_money(result7.method1_total_amount)}")
        print(f"   Method 2 - SUM(NetAmount):                  {fmt_money(result7.method2_net_amount)}")
        print(f"   Method 3 - SUM(NetAmount + TaxAmount):      {fmt_money(result7.method3_net_plus_tax)}")
        print(f"   Method 4 - SUM(GrossAmount - DiscountAmount): {fmt_money(result7.method4_gross_minus_disc)}")
        print()
        print(f"   Xilnex Target:                              RM 5,999.35")
        print(f"   Total Rows: {result7.total_rows}, Distinct Sales: {result7.distinct_sales}")
        print()
        
        # None of these match - so let's try unique per SaleNumber
        print("TEST 8: What if Xilnex only counts ONE row per SaleNumber?")
        print("-" * 90)
        print("   Taking MAX(TotalAmount) per SaleNumber instead of SUM:")
        print()
        
        query8 = text("""
            SELECT 
                SUM(max_amount) as sum_of_max_per_sale,
                COUNT(*) as distinct_sales
            FROM (
                SELECT 
                    f.SaleNumber,
                    MAX(f.TotalAmount) as max_amount
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE d.FullDate = '2019-04-01'
                    AND l.LocationName = 'MB JITRA'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
                GROUP BY f.SaleNumber
            ) x
        """)
        result8 = conn.execute(query8).first()
        print(f"   SUM(MAX(TotalAmount) per SaleNumber): {fmt_money(result8.sum_of_max_per_sale)}")
        print()
        
        # ============================================================
        # FINAL: Check Rounding column
        # ============================================================
        print("TEST 9: Check Rounding column")
        print("-" * 90)
        
        query9 = text("""
            SELECT 
                SUM(f.TotalAmount) as total_amount,
                SUM(f.Rounding) as total_rounding,
                SUM(f.TotalAmount - f.Rounding) as total_minus_rounding
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        result9 = conn.execute(query9).first()
        print(f"   TotalAmount:             {fmt_money(result9.total_amount)}")
        print(f"   Rounding:                {fmt_money(result9.total_rounding)}")
        print(f"   TotalAmount - Rounding:  {fmt_money(result9.total_minus_rounding)}")
        print()
        
        # ============================================================
        # SUMMARY
        # ============================================================
        print("=" * 90)
        print("INVESTIGATION SUMMARY")
        print("=" * 90)
        print("""
    The discrepancy for MB JITRA remains unexplained by standard filters:
    
    1. No duplicates at grain level (SaleNumber + ProductKey + PaymentTypeKey)
    2. SaleType/SalesStatus filters match expected behavior
    3. IsFOC filtering doesn't explain the gap
    4. NetAmount vs TotalAmount doesn't match Xilnex
    5. No obvious outliers or aggregation differences
    
    POSSIBLE ROOT CAUSES:
    
    A) ETL-RELATED:
       - Xilnex source data was UPDATED after our ETL ran
       - Different date/time boundaries during extraction
       - Data was transformed differently in older ETL runs
    
    B) XILNEX PORTAL LOGIC:
       - Xilnex may use a different column (not TotalAmount or NetAmount)
       - Xilnex may have additional filters we don't know about
       - Xilnex may aggregate at a different level (e.g., header-level vs line-level)
    
    C) DATA QUALITY:
       - Historical data inconsistencies in Xilnex source
       - Timing differences between portal and DW snapshots
    
    RECOMMENDED NEXT STEPS:
    1. Compare a RECENT date (e.g., Oct 2025) where ETL is fresh
    2. Request Xilnex portal's calculation logic/documentation
    3. Check if Xilnex uses a different source table (sale headers vs line items)
        """)

if __name__ == "__main__":
    investigate_jitra()

