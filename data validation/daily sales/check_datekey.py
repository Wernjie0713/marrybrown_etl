"""
Check DateKey distribution for MB JITRA around April 1, 2019
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

def check():
    engine = get_db_engine()
    
    print("=" * 90)
    print("DATEKEY DISTRIBUTION CHECK FOR MB JITRA")
    print("=" * 90)
    print()
    
    with engine.connect() as conn:
        # Check DateKey distribution for MB JITRA in April 2019
        print("1. DateKey distribution for MB JITRA (March 31 - April 2, 2019)")
        print("-" * 90)
        
        query1 = text("""
            SELECT 
                f.DateKey,
                d.FullDate,
                COUNT(*) as row_count,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND f.DateKey BETWEEN 20190331 AND 20190402
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.DateKey, d.FullDate
            ORDER BY f.DateKey
        """)
        result1 = conn.execute(query1).fetchall()
        
        print(f"   {'DateKey':<12} {'FullDate':<15} {'Rows':>10} {'Sales':>10} {'TotalAmount':>15}")
        print("   " + "-" * 62)
        for row in result1:
            print(f"   {row.DateKey:<12} {str(row.FullDate):<15} {row.row_count:>10,} {row.distinct_sales:>10,} {fmt_money(row.total_amount):>15}")
        print()
        
        # Check SaleNumber pattern
        print("2. SaleNumber patterns for April 1, 2019")
        print("-" * 90)
        
        query2 = text("""
            SELECT TOP 20
                f.SaleNumber,
                f.DateKey,
                f.TimeKey,
                COUNT(*) as line_items,
                SUM(f.TotalAmount) as sale_total
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY f.SaleNumber, f.DateKey, f.TimeKey
            ORDER BY CAST(f.SaleNumber AS BIGINT)
        """)
        result2 = conn.execute(query2).fetchall()
        
        print(f"   {'SaleNumber':<15} {'DateKey':<12} {'TimeKey':<10} {'LineItems':>10} {'SaleTotal':>15}")
        print("   " + "-" * 62)
        for row in result2:
            timekey = row.TimeKey if row.TimeKey is not None else 0
            print(f"   {row.SaleNumber:<15} {row.DateKey:<12} {timekey:<10} {row.line_items:>10} {fmt_money(row.sale_total):>15}")
        print()
        
        # Check the ratio for Xilnex target
        print("3. Calculating ratios")
        print("-" * 90)
        
        db_total = 23006.92
        xilnex_total = 5999.35
        ratio = db_total / xilnex_total
        
        print(f"   DB Total / Xilnex Total = {db_total} / {xilnex_total} = {ratio:.4f}")
        print(f"   This is approximately {ratio:.0f}x")
        print()
        
        # Check if dividing by 4 gives close to Xilnex
        print(f"   If we divide by 4: {db_total / 4:.2f}")
        print(f"   If we divide by 3.83: {db_total / ratio:.2f}")
        print()
        
        # Check distinct sales * average
        print("4. Breaking down the math")
        print("-" * 90)
        
        query4 = text("""
            SELECT 
                COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                SUM(f.TotalAmount) as total_amount,
                AVG(f.TotalAmount) as avg_line_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        result4 = conn.execute(query4).first()
        
        print(f"   Distinct Sales: {result4.distinct_sales}")
        print(f"   Total Amount: {fmt_money(result4.total_amount)}")
        print(f"   Avg Line Amount: {fmt_money(result4.avg_line_amount)}")
        print(f"   Expected if Xilnex uses avg * distinct sales: {float(result4.avg_line_amount or 0) * result4.distinct_sales:.2f}")
        print()
        
        # Check what columns might give us ~6000
        print("5. Trying different column combinations to match RM 5,999.35")
        print("-" * 90)
        
        query5 = text("""
            SELECT 
                SUM(f.TotalAmount) as total_amount,
                SUM(f.NetAmount) as net_amount,
                SUM(f.GrossAmount) as gross_amount,
                SUM(f.DiscountAmount) as discount_amount,
                SUM(f.TaxAmount) as tax_amount,
                SUM(f.CostAmount) as cost_amount,
                SUM(f.NetAmount - f.CostAmount) as profit,
                -- Possible derived values
                SUM(f.TotalAmount) / 4 as total_div_4,
                SUM(f.NetAmount) / 4 as net_div_4,
                COUNT(DISTINCT f.SaleNumber) as distinct_sales
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
        """)
        result5 = conn.execute(query5).first()
        
        print(f"   TotalAmount:        {fmt_money(result5.total_amount)}")
        print(f"   NetAmount:          {fmt_money(result5.net_amount)}")
        print(f"   GrossAmount:        {fmt_money(result5.gross_amount)}")
        print(f"   DiscountAmount:     {fmt_money(result5.discount_amount)}")
        print(f"   TaxAmount:          {fmt_money(result5.tax_amount)}")
        print(f"   CostAmount:         {fmt_money(result5.cost_amount)}")
        print(f"   Profit (Net-Cost):  {fmt_money(result5.profit)}")
        print(f"   TotalAmount / 4:    {fmt_money(result5.total_div_4)}")
        print(f"   NetAmount / 4:      {fmt_money(result5.net_div_4)}")
        print()
        print(f"   Xilnex Target:      RM 5,999.35")
        print()
        
        # CRITICAL: Check if data is duplicated at LocationKey level
        print("6. Check if there are multiple LocationKeys for 'MB JITRA'")
        print("-" * 90)
        
        query6 = text("""
            SELECT 
                l.LocationKey,
                l.LocationName,
                COUNT(*) as row_count,
                SUM(f.TotalAmount) as total_amount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            WHERE l.LocationName LIKE '%JITRA%'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
            GROUP BY l.LocationKey, l.LocationName
            ORDER BY l.LocationKey
        """)
        result6 = conn.execute(query6).fetchall()
        
        print(f"   {'LocationKey':>12} {'LocationName':<30} {'Rows':>10} {'TotalAmount':>15}")
        print("   " + "-" * 67)
        for row in result6:
            print(f"   {row.LocationKey:>12} {row.LocationName:<30} {row.row_count:>10,} {fmt_money(row.total_amount):>15}")
        print()
        
        # Check staging table if exists
        print("7. Check if there's data in staging tables")
        print("-" * 90)
        
        try:
            query7 = text("""
                SELECT COUNT(*) as count FROM dbo.stg_combined_sales WHERE 1=0
            """)
            conn.execute(query7)
            
            query7b = text("""
                SELECT 
                    COUNT(*) as row_count,
                    SUM(TotalAmount) as total_amount
                FROM dbo.stg_combined_sales
                WHERE CAST(SaleCreatedOn AS DATE) = '2019-04-01'
                    AND OutletName = 'MB JITRA'
            """)
            result7 = conn.execute(query7b).first()
            print(f"   Staging table stg_combined_sales exists")
            print(f"   Rows for MB JITRA on 2019-04-01: {result7.row_count}")
            print(f"   TotalAmount in staging: {fmt_money(result7.total_amount)}")
        except:
            print("   Staging table not found or empty")
        print()

if __name__ == "__main__":
    check()

