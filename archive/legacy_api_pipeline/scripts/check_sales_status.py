"""
Check Sales Status Filtering - October 2018
Investigate what statuses exist in API data

Author: YONG WERN JIE
Date: October 28, 2025
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Load environment
load_dotenv('.env.local')

# Create database connection
driver = os.getenv("TARGET_DRIVER").replace(" ", "+")
server = os.getenv("TARGET_SERVER")
database = os.getenv("TARGET_DATABASE")
user = os.getenv("TARGET_USERNAME")
password = os.getenv("TARGET_PASSWORD")

connection_uri = (
    f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    "&TrustServerCertificate=yes"
)
engine = create_engine(connection_uri, pool_pre_ping=True)

print("="*80)
print("SALES STATUS ANALYSIS - October 2018")
print("="*80)
print()

# 1. Overall status breakdown
print("1. Overall Status Breakdown (All Locations)")
print("-"*80)

query1 = text("""
    SELECT 
        ISNULL(f.SalesStatus, 'NULL') as SalesStatus,
        COUNT(DISTINCT f.SaleNumber) as TransactionCount,
        SUM(f.TotalAmount) as TotalSales,
        SUM(f.NetAmount - f.CostAmount) as TotalProfit
    FROM dbo.fact_sales_transactions_api f
    JOIN dbo.dim_date d ON f.DateKey = d.DateKey
    WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
    GROUP BY f.SalesStatus
    ORDER BY TransactionCount DESC
""")

with engine.connect() as conn:
    df1 = pd.read_sql(query1, conn)
    print(df1.to_string(index=False))
    print()

# 2. Status breakdown by location
print()
print("2. Status Breakdown by Location")
print("-"*80)

query2 = text("""
    SELECT 
        l.LocationName,
        ISNULL(f.SalesStatus, 'NULL') as SalesStatus,
        COUNT(DISTINCT f.SaleNumber) as TransactionCount,
        SUM(f.TotalAmount) as TotalSales,
        SUM(f.NetAmount - f.CostAmount) as TotalProfit
    FROM dbo.fact_sales_transactions_api f
    JOIN dbo.dim_date d ON f.DateKey = d.DateKey
    JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
    WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
    GROUP BY l.LocationName, f.SalesStatus
    ORDER BY l.LocationName, f.SalesStatus
""")

with engine.connect() as conn:
    df2 = pd.read_sql(query2, conn)
    print(df2.to_string(index=False))
    print()

# 3. Comparison: ALL statuses vs COMPLETED only
print()
print("3. Impact of Status Filtering (ALL vs COMPLETED)")
print("-"*80)

query3 = text("""
    WITH AllStatuses AS (
        SELECT 
            l.LocationName,
            SUM(f.TotalAmount) as TotalSales_All,
            SUM(f.NetAmount - f.CostAmount) as TotalProfit_All
        FROM dbo.fact_sales_transactions_api f
        JOIN dbo.dim_date d ON f.DateKey = d.DateKey
        JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
        WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
        GROUP BY l.LocationName
    ),
    CompletedOnly AS (
        SELECT 
            l.LocationName,
            SUM(f.TotalAmount) as TotalSales_Completed,
            SUM(f.NetAmount - f.CostAmount) as TotalProfit_Completed
        FROM dbo.fact_sales_transactions_api f
        JOIN dbo.dim_date d ON f.DateKey = d.DateKey
        JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
        WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
          AND f.SalesStatus = 'COMPLETED'
        GROUP BY l.LocationName
    )
    SELECT 
        a.LocationName,
        a.TotalSales_All,
        ISNULL(c.TotalSales_Completed, 0) as TotalSales_Completed,
        a.TotalSales_All - ISNULL(c.TotalSales_Completed, 0) as Sales_Difference,
        a.TotalProfit_All,
        ISNULL(c.TotalProfit_Completed, 0) as TotalProfit_Completed,
        a.TotalProfit_All - ISNULL(c.TotalProfit_Completed, 0) as Profit_Difference
    FROM AllStatuses a
    LEFT JOIN CompletedOnly c ON a.LocationName = c.LocationName
    ORDER BY a.LocationName
""")

with engine.connect() as conn:
    df3 = pd.read_sql(query3, conn)
    print(df3.to_string(index=False))
    print()

# 4. Check if SalesStatus is populated at all
print()
print("4. Check SalesStatus Population")
print("-"*80)

query4 = text("""
    SELECT 
        CASE 
            WHEN f.SalesStatus IS NULL THEN 'NULL values'
            WHEN f.SalesStatus = '' THEN 'Empty strings'
            ELSE 'Populated'
        END as StatusPopulation,
        COUNT(*) as RowCount,
        COUNT(DISTINCT f.SaleNumber) as UniqueTransactions
    FROM dbo.fact_sales_transactions_api f
    JOIN dbo.dim_date d ON f.DateKey = d.DateKey
    WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
    GROUP BY 
        CASE 
            WHEN f.SalesStatus IS NULL THEN 'NULL values'
            WHEN f.SalesStatus = '' THEN 'Empty strings'
            ELSE 'Populated'
        END
""")

with engine.connect() as conn:
    df4 = pd.read_sql(query4, conn)
    print(df4.to_string(index=False))
    print()

# 5. Sample transactions
print()
print("5. Sample Transactions (First 20)")
print("-"*80)

query5 = text("""
    SELECT TOP 20
        f.SaleNumber,
        d.FullDate as BusinessDate,
        l.LocationName,
        f.SalesStatus,
        f.SaleType,
        f.TotalAmount,
        f.NetAmount - f.CostAmount as Profit
    FROM dbo.fact_sales_transactions_api f
    JOIN dbo.dim_date d ON f.DateKey = d.DateKey
    JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
    WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
    ORDER BY f.SaleNumber
""")

with engine.connect() as conn:
    df5 = pd.read_sql(query5, conn)
    print(df5.to_string(index=False))
    print()

print()
print("="*80)
print("ANALYSIS COMPLETE")
print("="*80)
print()
print("KEY INSIGHTS:")
print("-"*80)

# Calculate if SalesStatus filtering is the issue
with engine.connect() as conn:
    df_analysis = pd.read_sql(query3, conn)
    
    for _, row in df_analysis.iterrows():
        store = row['LocationName']
        diff_pct = (row['Sales_Difference'] / row['TotalSales_All'] * 100) if row['TotalSales_All'] > 0 else 0
        
        print(f"{store}:")
        if abs(diff_pct) < 0.01:
            print(f"  - ALL = COMPLETED (no filtering needed)")
        else:
            print(f"  - {abs(diff_pct):.2f}% of data is non-COMPLETED status")
            print(f"  - Difference: RM {abs(row['Sales_Difference']):,.2f}")

