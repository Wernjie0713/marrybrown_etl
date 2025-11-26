"""
Investigate data discrepancies for April 1, 2019
Compare My Portal vs Xilnex Portal

Target stores with largest discrepancies:
1. MB JITRA: My Portal RM 23,006.92 vs Xilnex RM 5,999.35 (+283%)
2. MB ANGSANA: My Portal RM 3,385.65 vs Xilnex RM 2,938.05 (+15%)
3. MB CITTA MALL: My Portal RM 2,810.18 vs Xilnex RM 3,147.20 (-12%)

Usage:
    cd C:\laragon\www\marrybrown_etl
    python "data validation/daily sales/investigate_apr2019.py"
"""
import os
import sys
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path

# Load environment
load_dotenv('.env.local')

def get_db_engine():
    """Create database connection from .env.local"""
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
    """Format currency value"""
    if val is None:
        return "RM 0.00"
    return f"RM {val:,.2f}"

def investigate():
    """Run diagnostic queries for April 1, 2019"""
    engine = get_db_engine()
    
    print("=" * 90)
    print("DATA DISCREPANCY INVESTIGATION: April 1, 2019")
    print("=" * 90)
    print()
    
    # Load comparison data
    script_dir = Path(__file__).parent
    with open(script_dir / "my_portal.json", "r") as f:
        my_portal_data = json.load(f)
    with open(script_dir / "xilnex_portal.json", "r") as f:
        xilnex_data = json.load(f)
    
    # Create lookup dicts
    my_portal = {row["store_name"]: row["sales_amount"] for row in my_portal_data}
    xilnex = {row["store_name"]: row["sales_amount"] for row in xilnex_data}
    
    with engine.connect() as conn:
        # ============================================================
        # QUERY 1: Overall summary by SaleType and SalesStatus
        # ============================================================
        print("1. BREAKDOWN BY SALETYPE AND SALESSTATUS")
        print("-" * 90)
        print("   Formula: SUM(TotalAmount) grouped by SaleType, SalesStatus")
        print("   Purpose: Understand distribution of transaction types")
        print()
        
        query1 = text("""
            SELECT 
                f.SaleType,
                f.SalesStatus,
                COUNT(*) as row_count,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount,
                SUM(f.NetAmount) as net_amount,
                SUM(f.TaxAmount) as tax_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            WHERE d.FullDate = '2019-04-01'
            GROUP BY f.SaleType, f.SalesStatus
            ORDER BY f.SaleType, f.SalesStatus
        """)
        result1 = conn.execute(query1).fetchall()
        
        print(f"   {'SaleType':<20} {'SalesStatus':<15} {'Rows':>10} {'Sales':>10} {'TotalAmount':>15} {'NetAmount':>15} {'TaxAmount':>12}")
        print("   " + "-" * 97)
        for row in result1:
            print(f"   {row.SaleType or 'NULL':<20} {row.SalesStatus or 'NULL':<15} {row.row_count:>10,} {row.distinct_sales:>10,} {fmt_money(row.total_amount):>15} {fmt_money(row.net_amount):>15} {fmt_money(row.tax_amount):>12}")
        print()
        
        # ============================================================
        # QUERY 2: Store-level comparison with API filter logic
        # ============================================================
        print("2. STORE-LEVEL COMPARISON (API Filter: SaleType != 'Return', SalesStatus = 'COMPLETED')")
        print("-" * 90)
        print("   Formula: SUM(f.TotalAmount) WHERE SaleType != 'Return' AND SalesStatus = 'COMPLETED'")
        print("   This is EXACTLY what the API endpoint uses")
        print()
        
        query2 = text("""
            SELECT 
                l.LocationName as store_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount,
                SUM(f.NetAmount) as net_amount,
                SUM(f.TaxAmount) as tax_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY l.LocationName
            ORDER BY l.LocationName
        """)
        result2 = conn.execute(query2).fetchall()
        
        print(f"   {'Store':<35} {'Rows':>8} {'Sales':>8} {'DB TotalAmt':>14} {'Portal':>14} {'Xilnex':>14} {'Diff%':>8}")
        print("   " + "-" * 101)
        
        for row in result2:
            store = row.store_name
            db_amt = float(row.total_amount or 0)
            portal_amt = float(my_portal.get(store, 0))
            xilnex_amt = float(xilnex.get(store, 0))
            
            # Calculate diff percentage (vs Xilnex)
            if xilnex_amt > 0:
                diff_pct = ((db_amt - xilnex_amt) / xilnex_amt) * 100
            else:
                diff_pct = 0
            
            diff_str = f"{diff_pct:+.1f}%" if abs(diff_pct) > 0.1 else "OK"
            
            print(f"   {store:<35} {row.row_count:>8,} {row.distinct_sales:>8,} {fmt_money(db_amt):>14} {fmt_money(portal_amt):>14} {fmt_money(xilnex_amt):>14} {diff_str:>8}")
        print()
        
        # ============================================================
        # QUERY 3: Check for duplicates at grain level (MB JITRA focus)
        # ============================================================
        print("3. DUPLICATE CHECK FOR MB JITRA (SaleNumber + ProductKey + PaymentTypeKey)")
        print("-" * 90)
        print("   Formula: COUNT(*) > 1 for same SaleNumber + ProductKey + PaymentTypeKey")
        print("   Purpose: Detect if same sale item appears multiple times")
        print()
        
        query3 = text("""
            SELECT 
                f.SaleNumber,
                f.ProductKey,
                f.PaymentTypeKey,
                COUNT(*) as duplicate_count,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber, f.ProductKey, f.PaymentTypeKey
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        """)
        result3 = conn.execute(query3).fetchall()
        
        if result3:
            print(f"   [WARNING] Found {len(result3)} duplicate grain combinations!")
            print(f"   {'SaleNumber':<20} {'ProductKey':>12} {'PaymentKey':>12} {'Duplicates':>12} {'TotalAmount':>15}")
            print("   " + "-" * 71)
            for row in result3[:20]:  # Show first 20
                print(f"   {row.SaleNumber:<20} {row.ProductKey or 0:>12} {row.PaymentTypeKey or 0:>12} {row.duplicate_count:>12} {fmt_money(row.total_amount):>15}")
            if len(result3) > 20:
                print(f"   ... and {len(result3) - 20} more")
        else:
            print("   [OK] No duplicate grain combinations found")
        print()
        
        # ============================================================
        # QUERY 4: Sample raw transactions for MB JITRA
        # ============================================================
        print("4. SAMPLE RAW TRANSACTIONS FOR MB JITRA (First 20 rows)")
        print("-" * 90)
        print("   Purpose: Inspect actual row data to understand what's being summed")
        print()
        
        query4 = text("""
            SELECT TOP 20
                f.SaleNumber,
                f.SaleType,
                f.SubSalesType,
                f.SalesStatus,
                f.TotalAmount,
                f.NetAmount,
                f.TaxAmount,
                f.Quantity,
                p.ProductName
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            LEFT JOIN dbo.dim_products p ON f.ProductKey = p.ProductKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB JITRA'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            ORDER BY f.SaleNumber, f.TotalAmount DESC
        """)
        result4 = conn.execute(query4).fetchall()
        
        print(f"   {'SaleNumber':<12} {'SaleType':<10} {'SubSalesType':<15} {'Status':<12} {'TotalAmt':>12} {'NetAmt':>12} {'Qty':>6} {'Product':<25}")
        print("   " + "-" * 114)
        for row in result4:
            product = (row.ProductName or "N/A")[:25]
            print(f"   {row.SaleNumber:<12} {(row.SaleType or 'N/A'):<10} {(row.SubSalesType or 'N/A'):<15} {(row.SalesStatus or 'N/A'):<12} {fmt_money(row.TotalAmount):>12} {fmt_money(row.NetAmount):>12} {row.Quantity or 0:>6.0f} {product:<25}")
        print()
        
        # ============================================================
        # QUERY 5: Check distinct SaleNumber counts per store
        # ============================================================
        print("5. TRANSACTION COUNT ANALYSIS (Distinct SaleNumbers vs Total Rows)")
        print("-" * 90)
        print("   Formula: Ratio = Total Rows / Distinct SaleNumbers")
        print("   Purpose: High ratios suggest split payments or multiple line items per sale")
        print()
        
        query5 = text("""
            SELECT 
                l.LocationName as store_name,
                COUNT(*) as total_rows,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                CAST(COUNT(*) AS FLOAT) / NULLIF(COUNT(DISTINCT f.SaleNumber), 0) as ratio,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY l.LocationName
            ORDER BY CAST(COUNT(*) AS FLOAT) / NULLIF(COUNT(DISTINCT f.SaleNumber), 0) DESC
        """)
        result5 = conn.execute(query5).fetchall()
        
        print(f"   {'Store':<35} {'Total Rows':>12} {'Distinct Sales':>15} {'Ratio':>8} {'TotalAmount':>15}")
        print("   " + "-" * 85)
        for row in result5:
            ratio = row.ratio or 0
            flag = " [!]" if ratio > 10 else ""
            print(f"   {row.store_name:<35} {row.total_rows:>12,} {row.distinct_sales:>15,} {ratio:>8.2f} {fmt_money(row.total_amount):>15}{flag}")
        print()
        
        # ============================================================
        # QUERY 6: MB CITTA MALL (Portal LOWER than Xilnex)
        # ============================================================
        print("6. MB CITTA MALL DETAILED ANALYSIS (Portal shows LOWER than Xilnex)")
        print("-" * 90)
        print("   Portal: RM 2,810.18 | Xilnex: RM 3,147.20 | Difference: -RM 337.02")
        print("   Purpose: Check if transactions are missing or filtered differently")
        print()
        
        query6 = text("""
            SELECT 
                f.SaleType,
                f.SubSalesType,
                f.SalesStatus,
                COUNT(*) as row_count,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE d.FullDate = '2019-04-01'
                AND l.LocationName = 'MB CITTA MALL'
            GROUP BY f.SaleType, f.SubSalesType, f.SalesStatus
            ORDER BY f.SaleType, f.SubSalesType, f.SalesStatus
        """)
        result6 = conn.execute(query6).fetchall()
        
        print(f"   {'SaleType':<15} {'SubSalesType':<20} {'SalesStatus':<12} {'Rows':>8} {'Sales':>8} {'TotalAmount':>15}")
        print("   " + "-" * 78)
        grand_total = 0
        for row in result6:
            amt = row.total_amount or 0
            grand_total += amt
            print(f"   {(row.SaleType or 'NULL'):<15} {(row.SubSalesType or 'NULL'):<20} {(row.SalesStatus or 'NULL'):<12} {row.row_count:>8,} {row.distinct_sales:>8,} {fmt_money(amt):>15}")
        print("   " + "-" * 78)
        print(f"   {'GRAND TOTAL':<47} {'':<8} {'':<8} {fmt_money(grand_total):>15}")
        print()
        
        # ============================================================
        # QUERY 7: Check if SubSalesType affects totals
        # ============================================================
        print("7. SUBSALESTYPE ANALYSIS FOR ALL STORES")
        print("-" * 90)
        print("   Purpose: Check if Xilnex filters by SubSalesType")
        print()
        
        query7 = text("""
            SELECT 
                f.SubSalesType,
                COUNT(*) as row_count,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            WHERE d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SubSalesType
            ORDER BY SUM(f.TotalAmount) DESC
        """)
        result7 = conn.execute(query7).fetchall()
        
        print(f"   {'SubSalesType':<30} {'Rows':>12} {'Distinct Sales':>15} {'TotalAmount':>15}")
        print("   " + "-" * 72)
        for row in result7:
            print(f"   {(row.SubSalesType or 'NULL'):<30} {row.row_count:>12,} {row.distinct_sales:>15,} {fmt_money(row.total_amount):>15}")
        print()
        
        # ============================================================
        # SUMMARY
        # ============================================================
        print("=" * 90)
        print("SUMMARY OF FINDINGS")
        print("=" * 90)
        
        # Calculate totals
        query_total = text("""
            SELECT 
                SUM(f.TotalAmount) as db_total
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            WHERE d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        db_total = float(conn.execute(query_total).scalar() or 0)
        
        portal_total = float(sum(my_portal.values()))
        xilnex_total = float(sum(xilnex.values()))
        
        print(f"""
    Database Total (SaleType != 'Return', SalesStatus = 'COMPLETED'):
        Formula: SUM(TotalAmount) from fact_sales_transactions
        Result:  {fmt_money(db_total)}
    
    My Portal Total (from exported JSON):
        Result:  {fmt_money(portal_total)}
    
    Xilnex Portal Total (from exported JSON):
        Result:  {fmt_money(xilnex_total)}
    
    Discrepancy Analysis:
        DB vs Portal:  {fmt_money(db_total - portal_total)} ({((db_total - portal_total) / portal_total * 100):+.2f}%)
        DB vs Xilnex:  {fmt_money(db_total - xilnex_total)} ({((db_total - xilnex_total) / xilnex_total * 100):+.2f}%)
        Portal vs Xilnex: {fmt_money(portal_total - xilnex_total)} ({((portal_total - xilnex_total) / xilnex_total * 100):+.2f}%)
        """)
        print()

if __name__ == "__main__":
    investigate()

