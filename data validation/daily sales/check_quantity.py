"""
Check Quantity values - MB JITRA has 5,965 total qty vs 1,446 for Central I-City
This might be the root cause
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
    
    print("=" * 100)
    print("QUANTITY ANALYSIS")
    print("=" * 100)
    print()
    
    with engine.connect() as conn:
        # ============================================================
        # Check Quantity distribution
        # ============================================================
        print("1. Quantity distribution per row")
        print("-" * 100)
        
        for store in ['MB JITRA', 'MB CENTRAL I-CITY']:
            query = text("""
                SELECT 
                    CASE 
                        WHEN f.Quantity = 0 THEN 'Qty = 0'
                        WHEN f.Quantity = 1 THEN 'Qty = 1'
                        WHEN f.Quantity = 2 THEN 'Qty = 2'
                        WHEN f.Quantity BETWEEN 3 AND 5 THEN 'Qty 3-5'
                        WHEN f.Quantity BETWEEN 6 AND 10 THEN 'Qty 6-10'
                        ELSE 'Qty > 10'
                    END as qty_range,
                    COUNT(*) as row_count,
                    SUM(f.Quantity) as total_qty,
                    SUM(f.TotalAmount) as total_amount
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE l.LocationName = :store
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
                GROUP BY 
                    CASE 
                        WHEN f.Quantity = 0 THEN 'Qty = 0'
                        WHEN f.Quantity = 1 THEN 'Qty = 1'
                        WHEN f.Quantity = 2 THEN 'Qty = 2'
                        WHEN f.Quantity BETWEEN 3 AND 5 THEN 'Qty 3-5'
                        WHEN f.Quantity BETWEEN 6 AND 10 THEN 'Qty 6-10'
                        ELSE 'Qty > 10'
                    END
                ORDER BY 
                    CASE 
                        WHEN f.Quantity = 0 THEN 'Qty = 0'
                        WHEN f.Quantity = 1 THEN 'Qty = 1'
                        WHEN f.Quantity = 2 THEN 'Qty = 2'
                        WHEN f.Quantity BETWEEN 3 AND 5 THEN 'Qty 3-5'
                        WHEN f.Quantity BETWEEN 6 AND 10 THEN 'Qty 6-10'
                        ELSE 'Qty > 10'
                    END
            """)
            result = conn.execute(query, {"store": store}).fetchall()
            
            print(f"   {store}:")
            print(f"   {'Qty Range':<15} {'Rows':>10} {'Total Qty':>15} {'TotalAmount':>15}")
            print("   " + "-" * 55)
            for row in result:
                qty = row.total_qty or 0
                print(f"   {row.qty_range:<15} {row.row_count:>10,} {qty:>15,.0f} {fmt_money(row.total_amount):>15}")
            print()
        
        # ============================================================
        # Check high quantity rows specifically
        # ============================================================
        print("2. High quantity rows (Qty > 5) for MB JITRA")
        print("-" * 100)
        
        query = text("""
            SELECT TOP 20
                f.SaleNumber,
                p.ProductName,
                f.Quantity,
                f.TotalAmount,
                f.NetAmount
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            LEFT JOIN dbo.dim_products p ON f.ProductKey = p.ProductKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
                AND f.Quantity > 5
            ORDER BY f.Quantity DESC
        """)
        result = conn.execute(query).fetchall()
        
        print(f"   {'SaleNumber':<15} {'Product':<35} {'Qty':>6} {'TotalAmount':>12} {'NetAmount':>12}")
        print("   " + "-" * 80)
        for row in result:
            product = (row.ProductName or "NULL")[:35]
            qty = row.Quantity or 0
            print(f"   {row.SaleNumber:<15} {product:<35} {qty:>6.0f} {fmt_money(row.TotalAmount):>12} {fmt_money(row.NetAmount):>12}")
        print()
        
        # ============================================================
        # KEY INSIGHT: What if TotalAmount includes Quantity already?
        # ============================================================
        print("3. Check if TotalAmount already includes Quantity multiplier")
        print("-" * 100)
        print("   If TotalAmount = UnitPrice * Quantity, then summing is correct")
        print("   If TotalAmount = UnitPrice (not multiplied), and we're multiplying by Quantity,")
        print("   then we're double-counting")
        print()
        
        # Sample a few rows to check
        query = text("""
            SELECT TOP 10
                f.SaleNumber,
                p.ProductName,
                f.Quantity,
                f.GrossAmount,
                f.DiscountAmount,
                f.NetAmount,
                f.TaxAmount,
                f.TotalAmount,
                -- Calculate implied unit price
                CASE WHEN f.Quantity > 0 THEN f.TotalAmount / f.Quantity ELSE f.TotalAmount END as implied_unit_price,
                CASE WHEN f.Quantity > 0 THEN f.NetAmount / f.Quantity ELSE f.NetAmount END as implied_unit_net
            FROM dbo.fact_sales_transactions f
            JOIN dbo.dim_date d ON f.DateKey = d.DateKey
            JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
            LEFT JOIN dbo.dim_products p ON f.ProductKey = p.ProductKey
            WHERE l.LocationName = 'MB JITRA'
                AND d.FullDate = '2019-04-01'
                AND f.SaleType != 'Return'
                AND f.SalesStatus = 'COMPLETED'
                AND f.Quantity > 1
                AND f.TotalAmount > 0
            ORDER BY f.Quantity DESC
        """)
        result = conn.execute(query).fetchall()
        
        print(f"   {'Product':<30} {'Qty':>4} {'Gross':>10} {'Net':>10} {'Tax':>8} {'Total':>10} {'Unit$':>8}")
        print("   " + "-" * 80)
        for row in result:
            product = (row.ProductName or "NULL")[:30]
            qty = row.Quantity or 0
            unit = float(row.implied_unit_price or 0)
            print(f"   {product:<30} {qty:>4.0f} {fmt_money(row.GrossAmount):>10} {fmt_money(row.NetAmount):>10} {fmt_money(row.TaxAmount):>8} {fmt_money(row.TotalAmount):>10} {fmt_money(unit):>8}")
        print()
        print("   Observation: If Unit$ looks like a reasonable menu price, TotalAmount already includes Qty")
        print()
        
        # ============================================================
        # FINAL TEST: What if we SUM(TotalAmount / Quantity)?
        # ============================================================
        print("4. What if we calculate SUM(TotalAmount / Quantity)?")
        print("-" * 100)
        
        for store, xilnex_total in [('MB JITRA', 5999.35), ('MB CENTRAL I-CITY', 4540.75)]:
            query = text("""
                SELECT 
                    SUM(f.TotalAmount) as total_amount,
                    SUM(CASE WHEN f.Quantity > 0 THEN f.TotalAmount / f.Quantity ELSE f.TotalAmount END) as sum_unit_prices,
                    SUM(f.Quantity) as total_qty
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE l.LocationName = :store
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
            """)
            result = conn.execute(query, {"store": store}).first()
            
            db_total = float(result.total_amount or 0)
            sum_units = float(result.sum_unit_prices or 0)
            
            print(f"   {store}:")
            print(f"   - SUM(TotalAmount):              {fmt_money(db_total)}")
            print(f"   - SUM(TotalAmount/Quantity):     {fmt_money(sum_units)}")
            print(f"   - Xilnex Total:                  {fmt_money(xilnex_total)}")
            print(f"   - Total Quantity:                {result.total_qty:,.0f}")
            print()
            
            # Check if dividing by average qty helps
            if result.total_qty > 0:
                avg_qty = result.total_qty / 1158  # approx rows
                print(f"   - If we divide by avg qty ({avg_qty:.2f}): {fmt_money(db_total / avg_qty)}")
            print()

if __name__ == "__main__":
    check()

