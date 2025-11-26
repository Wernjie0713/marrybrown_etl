"""
Compare MB JITRA (mismatched) vs MB CENTRAL I-CITY (matched) to find the difference
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

def compare():
    engine = get_db_engine()
    
    print("=" * 100)
    print("STORE COMPARISON: MB JITRA (mismatched) vs MB CENTRAL I-CITY (matched)")
    print("=" * 100)
    print()
    
    with engine.connect() as conn:
        # ============================================================
        # Check ProductKey distribution
        # ============================================================
        print("1. ProductKey value distribution")
        print("-" * 100)
        
        for store in ['MB JITRA', 'MB CENTRAL I-CITY']:
            query = text("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                    COUNT(DISTINCT f.ProductKey) as distinct_products,
                    SUM(CASE WHEN f.ProductKey IS NULL THEN 1 ELSE 0 END) as null_product_keys,
                    SUM(f.TotalAmount) as total_amount,
                    SUM(f.Quantity) as total_quantity
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE l.LocationName = :store
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
            """)
            result = conn.execute(query, {"store": store}).first()
            
            print(f"   {store}:")
            print(f"   - Total Rows:          {result.total_rows:,}")
            print(f"   - Distinct Sales:      {result.distinct_sales:,}")
            print(f"   - Distinct Products:   {result.distinct_products:,}")
            print(f"   - NULL ProductKeys:    {result.null_product_keys:,}")
            print(f"   - Total Quantity:      {result.total_quantity:,.0f}")
            print(f"   - Total Amount:        {fmt_money(result.total_amount)}")
            print(f"   - Rows per Sale:       {result.total_rows / result.distinct_sales:.2f}")
            print()
        
        # ============================================================
        # Check if TotalAmount = 0 rows exist
        # ============================================================
        print("2. Distribution of TotalAmount values")
        print("-" * 100)
        
        for store in ['MB JITRA', 'MB CENTRAL I-CITY']:
            query = text("""
                SELECT 
                    CASE 
                        WHEN f.TotalAmount = 0 THEN 'Zero'
                        WHEN f.TotalAmount < 0 THEN 'Negative'
                        WHEN f.TotalAmount < 10 THEN 'Under RM 10'
                        WHEN f.TotalAmount < 50 THEN 'RM 10-50'
                        WHEN f.TotalAmount < 100 THEN 'RM 50-100'
                        ELSE 'RM 100+'
                    END as amount_range,
                    COUNT(*) as row_count,
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
                        WHEN f.TotalAmount = 0 THEN 'Zero'
                        WHEN f.TotalAmount < 0 THEN 'Negative'
                        WHEN f.TotalAmount < 10 THEN 'Under RM 10'
                        WHEN f.TotalAmount < 50 THEN 'RM 10-50'
                        WHEN f.TotalAmount < 100 THEN 'RM 50-100'
                        ELSE 'RM 100+'
                    END
                ORDER BY SUM(f.TotalAmount)
            """)
            result = conn.execute(query, {"store": store}).fetchall()
            
            print(f"   {store}:")
            for row in result:
                print(f"   - {row.amount_range:<15}: {row.row_count:>5} rows, {fmt_money(row.total_amount):>15}")
            print()
        
        # ============================================================
        # KEY TEST: Check if same SaleNumber appears in multiple ETL batches
        # ============================================================
        print("3. Check for potential ETL duplication (CreatedAt timestamps)")
        print("-" * 100)
        
        for store in ['MB JITRA', 'MB CENTRAL I-CITY']:
            query = text("""
                SELECT 
                    CAST(f.CreatedAt AS DATE) as created_date,
                    COUNT(*) as row_count,
                    COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                    SUM(f.TotalAmount) as total_amount
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE l.LocationName = :store
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
                GROUP BY CAST(f.CreatedAt AS DATE)
                ORDER BY CAST(f.CreatedAt AS DATE)
            """)
            result = conn.execute(query, {"store": store}).fetchall()
            
            print(f"   {store}:")
            if result:
                for row in result:
                    created = row.created_date if row.created_date else "NULL"
                    print(f"   - Created: {created}, Rows: {row.row_count:,}, Sales: {row.distinct_sales:,}, Amount: {fmt_money(row.total_amount)}")
            else:
                print("   - No data found")
            print()
        
        # ============================================================
        # Check the actual SaleIDs/SaleNumbers to see pattern
        # ============================================================
        print("4. SaleNumber ranges")
        print("-" * 100)
        
        for store in ['MB JITRA', 'MB CENTRAL I-CITY']:
            query = text("""
                SELECT 
                    MIN(CAST(f.SaleNumber AS BIGINT)) as min_sale_num,
                    MAX(CAST(f.SaleNumber AS BIGINT)) as max_sale_num,
                    COUNT(DISTINCT f.SaleNumber) as distinct_sales
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                WHERE l.LocationName = :store
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
            """)
            result = conn.execute(query, {"store": store}).first()
            
            print(f"   {store}:")
            print(f"   - Min SaleNumber: {result.min_sale_num}")
            print(f"   - Max SaleNumber: {result.max_sale_num}")
            print(f"   - Distinct Sales: {result.distinct_sales}")
            print(f"   - Range:          {result.max_sale_num - result.min_sale_num + 1} (if sequential)")
            print()
        
        # ============================================================
        # CRITICAL: What if Xilnex uses a different field like "GrandTotal" at sale header level?
        # ============================================================
        print("5. Compare SUM(TotalAmount) vs COUNT(DISTINCT SaleNumber) * Avg Sale Amount")
        print("-" * 100)
        
        # For matched store (Central I-City), what's the relationship?
        for store, xilnex_total in [('MB CENTRAL I-CITY', 4540.75), ('MB JITRA', 5999.35)]:
            query = text("""
                SELECT 
                    COUNT(DISTINCT f.SaleNumber) as distinct_sales,
                    SUM(f.TotalAmount) as total_amount,
                    AVG(f.TotalAmount) as avg_line_amount
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
            
            print(f"   {store}:")
            print(f"   - DB SUM(TotalAmount):  {fmt_money(db_total)}")
            print(f"   - Xilnex Total:         {fmt_money(xilnex_total)}")
            print(f"   - Difference:           {fmt_money(db_total - xilnex_total)} ({((db_total - xilnex_total) / xilnex_total * 100):+.1f}%)")
            
            # If Xilnex is correct, what's the implied average per sale?
            implied_avg = xilnex_total / result.distinct_sales
            print(f"   - Distinct Sales:       {result.distinct_sales}")
            print(f"   - Implied Avg/Sale:     {fmt_money(implied_avg)}")
            print()
        
        # ============================================================
        # THEORY: Maybe the issue is with specific products or categories
        # ============================================================
        print("6. Top products by TotalAmount contribution")
        print("-" * 100)
        
        for store in ['MB JITRA', 'MB CENTRAL I-CITY']:
            query = text("""
                SELECT TOP 10
                    p.ProductName,
                    COUNT(*) as occurrences,
                    SUM(f.TotalAmount) as total_amount,
                    SUM(f.Quantity) as total_quantity
                FROM dbo.fact_sales_transactions f
                JOIN dbo.dim_date d ON f.DateKey = d.DateKey
                JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
                LEFT JOIN dbo.dim_products p ON f.ProductKey = p.ProductKey
                WHERE l.LocationName = :store
                    AND d.FullDate = '2019-04-01'
                    AND f.SaleType != 'Return'
                    AND f.SalesStatus = 'COMPLETED'
                GROUP BY p.ProductName
                ORDER BY SUM(f.TotalAmount) DESC
            """)
            result = conn.execute(query, {"store": store}).fetchall()
            
            print(f"   {store}:")
            print(f"   {'Product':<40} {'Count':>8} {'Qty':>8} {'TotalAmount':>15}")
            print("   " + "-" * 71)
            for row in result:
                product = (row.ProductName or "NULL")[:40]
                qty = row.total_quantity or 0
                print(f"   {product:<40} {row.occurrences:>8} {qty:>8.0f} {fmt_money(row.total_amount):>15}")
            print()

if __name__ == "__main__":
    compare()

