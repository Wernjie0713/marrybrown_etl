"""
Transform API staging data to fact_sales_transactions
Applies split-tender payment allocation logic identical to production ETL

Author: YONG WERN JIE
Date: October 29, 2025 (Updated for Cloud Deployment)
"""

import os
import sys
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import time
import traceback

# Ensure project root on sys.path so `monitoring` and other packages can be imported
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Removed: from monitoring import DataQualityValidator
from utils.env_loader import load_environment
from utils.db_connection import get_warehouse_engine

# Load environment - use .env.local for local development
load_environment(force_local=True)

STAGING_RETENTION_DAYS = int(os.getenv("STAGING_RETENTION_DAYS", "14"))


def cleanup_staging(retention_days: int) -> None:
    """
    Purge staging data older than the retention window to keep merges fast.
    """
    if retention_days <= 0:
        return
        
    cutoff_sql = text("""
        DECLARE @cutoff DATETIME = DATEADD(day, -:days, CAST(GETDATE() AS DATE));
        DELETE FROM dbo.staging_sales WHERE BusinessDateTime < @cutoff;
        DELETE si
        FROM dbo.staging_sales_items si
        INNER JOIN dbo.staging_sales ss ON ss.SaleID = si.SaleID
        WHERE ss.BusinessDateTime < @cutoff;
        DELETE sp
        FROM dbo.staging_payments sp
        INNER JOIN dbo.staging_sales ss ON ss.SaleID = sp.SaleID
        WHERE ss.BusinessDateTime < @cutoff;
    """)
    
    try:
        engine = get_warehouse_engine()
        with engine.begin() as conn:
            print(f"[STAGING] Purging entries older than {retention_days} day(s)...")
            conn.execute(cutoff_sql, {"days": retention_days})
            print("  [STAGING] Retention cleanup complete.")
    except Exception as e:
        print(f"[WARNING] Staging cleanup failed: {e}")
        traceback.print_exc()
        # Don't fail the entire ETL process for cleanup failure


def transform_to_facts_optimized(chunk_size=10000) -> None:
    """
    OPTIMIZED: Transform staging to facts using chunked MERGE for deduplication
    
    Benefits over DELETE+INSERT approach:
    - No DELETE required (MERGE handles updates atomically)
    - Proper deduplication at fact level using composite key
    - Single atomic transaction (better safety)
    - Best performance for incremental loads
    - Append mode safe (no data loss risk)
    - Chunked processing prevents memory issues and improves performance
    
    Processes ALL staging data in chunks - MERGE automatically handles deduplication.
    No date range filtering - extraction uses timestamp-based pagination.
    
    Args:
        chunk_size: Number of staging sales records to process per chunk (default: 10000)
    """
    print("="*80)
    print("TRANSFORMING TO FACT TABLE (OPTIMIZED CHUNKED MERGE)")
    print("="*80)
    print("Processing ALL staging data (timestamp-based, no date range)")
    print(f"Chunk size: {chunk_size:,} sales records per chunk")
    print()
    
    engine = get_warehouse_engine()
    
    start_time = time.time()
    
    # Get total count of staging sales to determine chunks
    with engine.begin() as conn:
        total_count_result = conn.execute(text("""
            SELECT COUNT(*) FROM dbo.staging_sales
        """)).fetchone()
        total_count = total_count_result[0] if total_count_result else 0
    
    if total_count == 0:
        print("  [INFO] No staging data to process.")
        return
    
    total_chunks = (total_count // chunk_size) + (1 if total_count % chunk_size > 0 else 0)
    print(f"  [INFO] Processing {total_count:,} sales records in {total_chunks} chunk(s)")
    print()
    
    # Process in chunks
    for chunk_num in range(total_chunks):
        offset = chunk_num * chunk_size
        print(f"[Chunk {chunk_num + 1}/{total_chunks}] Processing sales {offset + 1:,} to {min(offset + chunk_size, total_count):,}...")
        
        chunk_start_time = time.time()  # CORRECT: Track time per chunk
        
        with engine.begin() as conn:
            try:
                # Use MERGE instead of DELETE + INSERT
                # Aggregate to SaleID + Product + Payment type granularity to match fact uniqueness
                # Process only this chunk using ROW_NUMBER() for pagination
                merge_result = conn.execute(text("""
                WITH ChunkedSales AS (
                    -- Select only this chunk of sales using ROW_NUMBER()
                    -- Ordered by BusinessDateTime to ensure TransactionKey follows chronological order
                    SELECT ss.*
                    FROM (
                        SELECT ss.*, 
                               ROW_NUMBER() OVER (ORDER BY ss.BusinessDateTime, ss.SaleID) as rn
                        FROM dbo.staging_sales ss
                    ) ss
                    WHERE ss.rn > :offset AND ss.rn <= :offset_plus_chunk
                ),
                PaymentAllocations AS (
                    -- Calculate allocation percentage for each payment method per sale
                    SELECT
                        sp.SaleID,
                        sp.PaymentMethod,
                        sp.CardType,
                        sp.Amount as payment_amount,
                        sp.PaymentReference,
                        sp.EODSessionID,
                        SUM(sp.Amount) OVER (PARTITION BY sp.SaleID) as total_payment_amount,
                        -- Calculate what percentage of total payment this method represents
                        CASE
                            WHEN SUM(sp.Amount) OVER (PARTITION BY sp.SaleID) > 0
                            THEN sp.Amount / SUM(sp.Amount) OVER (PARTITION BY sp.SaleID)
                            ELSE 0
                        END as allocation_percentage
                    FROM dbo.staging_payments sp
                    INNER JOIN ChunkedSales cs ON sp.SaleID = cs.SaleID
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dbo.staging_payments sp2
                        WHERE sp2.SaleID = sp.SaleID AND sp2.IsVoid = 1
                    )
                ),
                TransformedData AS (
                    SELECT
                        CAST(cs.SaleID AS VARCHAR(45)) as SaleNumber,

                        -- Date and Time Keys
                        CAST(FORMAT(CAST(cs.BusinessDateTime AS DATE), 'yyyyMMdd') AS INT) as DateKey,
                        CAST(REPLACE(CAST(CAST(cs.SystemDateTime AS TIME) AS VARCHAR(8)), ':', '') AS INT) as TimeKey,
                        
                        -- Dimension Keys
                        ISNULL(dl.LocationKey, -1) as LocationKey,
                        ISNULL(dp.ProductKey, -1) as ProductKey,
                        ISNULL(dc.CustomerKey, -1) as CustomerKey,
                        ISNULL(ds.StaffKey, -1) as StaffKey,
                        -1 as PromotionKey,
                        ISNULL(dpt.PaymentTypeKey, -1) as PaymentTypeKey,
                        ISNULL(dt.TerminalKey, -1) as TerminalKey,
                        
                        -- Transaction Details
                        cs.SalesType as SaleType,
                        cs.SubSalesType as SubSalesType,
                        cs.Status as SalesStatus,
                        cs.OrderSource as OrderSource,
                        
                        -- Measures (allocated by payment method)
                        si.Quantity,
                        si.Subtotal * pa.allocation_percentage as GrossAmount,
                        si.DiscountAmount * pa.allocation_percentage as DiscountAmount,
                        si.NetAmount * pa.allocation_percentage as NetAmount,
                        si.TaxAmount * pa.allocation_percentage as TaxAmount,
                        si.TotalAmount * pa.allocation_percentage as TotalAmount,
                        si.CostAmount * pa.allocation_percentage as CostAmount,
                        
                        -- API-specific fields
                        pa.CardType,
                        si.TaxCode,
                        si.TaxRate,
                        si.IsFOC,
                        cs.Rounding * pa.allocation_percentage as Rounding,
                        si.Model,
                        si.IsServiceCharge
                        
                    FROM ChunkedSales cs
                    JOIN dbo.staging_sales_items si ON cs.SaleID = si.SaleID
                    JOIN PaymentAllocations pa ON cs.SaleID = pa.SaleID
                    
                    -- Dimension Joins
                    LEFT JOIN dbo.dim_products dp ON si.ProductID = dp.SourceProductID
                    LEFT JOIN dbo.dim_locations dl ON cs.OutletName = dl.LocationName
                    LEFT JOIN dbo.dim_staff ds ON cs.CashierName = ds.StaffFullName
                    LEFT JOIN dbo.dim_customers dc ON cs.CustomerID = dc.CustomerGUID
                    LEFT JOIN dbo.dim_terminals dt ON cs.TerminalCode = dt.TerminalID
                    LEFT JOIN dbo.dim_payment_types dpt ON pa.PaymentMethod = dpt.PaymentMethodName
                    
                    WHERE pa.allocation_percentage > 0
                ),
                AggregatedData AS (
                    SELECT
                        SaleNumber,
                        DateKey,
                        MIN(TimeKey) as TimeKey,
                        MIN(LocationKey) as LocationKey,
                        ProductKey,
                        MIN(CustomerKey) as CustomerKey,
                        MIN(StaffKey) as StaffKey,
                        MIN(PromotionKey) as PromotionKey,
                        PaymentTypeKey,
                        MIN(TerminalKey) as TerminalKey,
                        -- Transaction Details
                        MIN(SaleType) as SaleType,
                        MIN(SubSalesType) as SubSalesType,
                        MIN(SalesStatus) as SalesStatus,
                        MIN(OrderSource) as OrderSource,
                        -- Measures
                        SUM(Quantity) as Quantity,
                        SUM(GrossAmount) as GrossAmount,
                        SUM(DiscountAmount) as DiscountAmount,
                        SUM(NetAmount) as NetAmount,
                        SUM(TaxAmount) as TaxAmount,
                        SUM(TotalAmount) as TotalAmount,
                        SUM(CostAmount) as CostAmount,
                        -- API-specific fields
                        MAX(CardType) as CardType,
                        MAX(TaxCode) as TaxCode,
                        MAX(TaxRate) as TaxRate,
                        CAST(MAX(CAST(IsFOC AS INT)) AS BIT) as IsFOC,
                        SUM(Rounding) as Rounding,
                        MAX(Model) as Model,
                        CAST(MAX(CAST(IsServiceCharge AS INT)) AS BIT) as IsServiceCharge
                    FROM TransformedData
                    GROUP BY
                        SaleNumber,
                        DateKey,
                        ProductKey,
                        PaymentTypeKey
                )
                MERGE dbo.fact_sales_transactions AS target
                USING AggregatedData AS source
                ON target.SaleNumber = source.SaleNumber
                   AND target.DateKey = source.DateKey
                   AND target.ProductKey = source.ProductKey
                   AND target.PaymentTypeKey = source.PaymentTypeKey
                WHEN MATCHED THEN
                    UPDATE SET
                        TimeKey = source.TimeKey,
                        LocationKey = source.LocationKey,
                        CustomerKey = source.CustomerKey,
                        StaffKey = source.StaffKey,
                        PromotionKey = source.PromotionKey,
                        TerminalKey = source.TerminalKey,
                        SaleType = source.SaleType,
                        SubSalesType = source.SubSalesType,
                        SalesStatus = source.SalesStatus,
                        OrderSource = source.OrderSource,
                        Quantity = source.Quantity,
                        GrossAmount = source.GrossAmount,
                        DiscountAmount = source.DiscountAmount,
                        NetAmount = source.NetAmount,
                        TaxAmount = source.TaxAmount,
                        TotalAmount = source.TotalAmount,
                        CostAmount = source.CostAmount,
                        CardType = source.CardType,
                        TaxCode = source.TaxCode,
                        TaxRate = source.TaxRate,
                        IsFOC = source.IsFOC,
                        Rounding = source.Rounding,
                        Model = source.Model,
                        IsServiceCharge = source.IsServiceCharge
                WHEN NOT MATCHED THEN
                    INSERT (DateKey, TimeKey, LocationKey, ProductKey, CustomerKey, StaffKey,
                            PromotionKey, PaymentTypeKey, TerminalKey, SaleNumber, SaleType,
                            SubSalesType, SalesStatus, OrderSource, Quantity, GrossAmount,
                            DiscountAmount, NetAmount, TaxAmount, TotalAmount, CostAmount,
                            CardType, TaxCode, TaxRate, IsFOC, Rounding, Model, IsServiceCharge)
                    VALUES (source.DateKey, source.TimeKey, source.LocationKey, source.ProductKey,
                            source.CustomerKey, source.StaffKey, source.PromotionKey,
                            source.PaymentTypeKey, source.TerminalKey, source.SaleNumber,
                            source.SaleType, source.SubSalesType, source.SalesStatus,
                            source.OrderSource, source.Quantity, source.GrossAmount,
                            source.DiscountAmount, source.NetAmount, source.TaxAmount,
                            source.TotalAmount, source.CostAmount, source.CardType,
                            source.TaxCode, source.TaxRate, source.IsFOC, source.Rounding,
                            source.Model, source.IsServiceCharge);
                """), {
                    "offset": offset,
                    "offset_plus_chunk": offset + chunk_size
                })
                
                chunk_time = time.time() - chunk_start_time  # CORRECT: Calculate against chunk_start_time
                rows_affected = merge_result.rowcount if merge_result.rowcount is not None else 0
                print(f"  ✓ Chunk complete: {rows_affected:,} rows affected ({chunk_time:.1f}s)")
                
            except Exception as e:
                print(f"  [ERROR] Chunk {chunk_num + 1} failed: {e}")
                traceback.print_exc()
                raise
    
    elapsed_time = time.time() - start_time
    
    print()
    print("="*80)
    print("TRANSFORMATION COMPLETE")
    print("="*80)
    print(f"  Total time: {elapsed_time:.2f} seconds")
    print(f"  Average per chunk: {elapsed_time/total_chunks:.2f} seconds" if total_chunks > 0 else "")
    print()

    if STAGING_RETENTION_DAYS > 0:
        cleanup_staging(STAGING_RETENTION_DAYS)


def transform_to_facts():
    """
    LEGACY: Transform data from staging tables to fact_sales_transactions
    Uses split-tender allocation logic identical to production ETL
    
    This is the old approach (TRUNCATE + INSERT) kept for backward compatibility.
    For new code, use transform_to_facts_optimized() instead.
    """
    print("="*80)
    print("TRANSFORMING API DATA TO FACT TABLE (LEGACY MODE)")
    print("="*80)
    print()
    
    engine = get_warehouse_engine()
    
    with engine.begin() as conn:  # Use begin() for automatic transaction management
        # Step 1: Transform and load with split-tender allocation
        print("Step 1: Transforming and loading data (no truncate)...")
        print("  - Applying split-tender allocation logic...")
        print("  - Populating new API-specific fields...")
        
        start_time = time.time()
        
        transform_query = text("""
            -- Transform API staging data to fact table with split-tender allocation
            WITH PaymentAllocations AS (
                -- Calculate allocation percentage for each payment method per sale
                SELECT
                    sp.SaleID,
                    sp.PaymentMethod,
                    sp.CardType,
                    sp.Amount as payment_amount,
                    sp.PaymentReference,
                    sp.EODSessionID,
                    SUM(sp.Amount) OVER (PARTITION BY sp.SaleID) as total_payment_amount,
                    -- Calculate what percentage of total payment this method represents
                    CASE
                        WHEN SUM(sp.Amount) OVER (PARTITION BY sp.SaleID) > 0
                        THEN sp.Amount / SUM(sp.Amount) OVER (PARTITION BY sp.SaleID)
                        ELSE 0
                    END as allocation_percentage
                FROM dbo.staging_payments sp
            )
            INSERT INTO dbo.fact_sales_transactions (
                DateKey, TimeKey,
                LocationKey, ProductKey, CustomerKey, StaffKey,
                PromotionKey, PaymentTypeKey, TerminalKey,
                SaleNumber, SaleType, SubSalesType, SalesStatus, OrderSource,
                Quantity, GrossAmount, DiscountAmount, NetAmount,
                TaxAmount, TotalAmount, CostAmount, CardType,
                -- NEW API-SPECIFIC FIELDS
                TaxCode, TaxRate, IsFOC, Rounding, Model, IsServiceCharge
            )
            SELECT
                -- Date and Time Keys
                CAST(FORMAT(CAST(ss.BusinessDateTime AS DATE), 'yyyyMMdd') AS INT) as DateKey,
                CAST(REPLACE(CAST(CAST(ss.SystemDateTime AS TIME) AS VARCHAR(8)), ':', '') AS INT) as TimeKey,
                
                -- Dimension Keys (with lookups)
                ISNULL(dl.LocationKey, -1) as LocationKey,
                ISNULL(dp.ProductKey, -1) as ProductKey,
                -1 as CustomerKey,  -- API doesn't have full customer details
                ISNULL(ds.StaffKey, -1) as StaffKey,
                -1 as PromotionKey,  -- Simplified for testing
                ISNULL(dpt.PaymentTypeKey, -1) as PaymentTypeKey,
                -1 as TerminalKey,  -- API doesn't provide terminal info
                
                -- Sale Information
                CAST(ss.SaleID AS VARCHAR(45)) as SaleNumber,
                ss.SalesType as SaleType,
                ss.SubSalesType as SubSalesType,
                ss.Status as SalesStatus,
                NULL as OrderSource,
                
                -- Measures (with split-tender allocation)
                si.Quantity,
                si.Subtotal * pa.allocation_percentage as GrossAmount,
                si.DiscountAmount * pa.allocation_percentage as DiscountAmount,
                si.NetAmount * pa.allocation_percentage as NetAmount,
                si.TaxAmount * pa.allocation_percentage as TaxAmount,
                si.TotalAmount * pa.allocation_percentage as TotalAmount,
                si.CostAmount * pa.allocation_percentage as CostAmount,  -- CORRECTED: Use CostAmount column
                pa.CardType,
                
                -- NEW API-SPECIFIC FIELDS
                si.TaxCode,
                si.TaxRate,
                si.IsFOC,
                ss.Rounding * pa.allocation_percentage as Rounding,
                si.Model,
                si.IsServiceCharge
                
            FROM dbo.staging_sales ss
            JOIN dbo.staging_sales_items si ON ss.SaleID = si.SaleID
            JOIN PaymentAllocations pa ON ss.SaleID = pa.SaleID
            
            -- Dimension Lookups
            LEFT JOIN dbo.dim_products dp 
                ON si.ProductID = dp.SourceProductID
            LEFT JOIN dbo.dim_locations dl
                ON ss.OutletName = dl.LocationName
            LEFT JOIN dbo.dim_staff ds 
                ON ss.CashierName = ds.StaffFullName
            LEFT JOIN dbo.dim_payment_types dpt 
                ON pa.PaymentMethod = dpt.PaymentMethodName
            
            WHERE pa.allocation_percentage > 0  -- Only include records with actual payment allocation
        """)
        
        result = conn.execute(transform_query)
        # No need to call conn.commit() - begin() context manager handles it automatically
        
        elapsed_time = time.time() - start_time
        rows_inserted = result.rowcount
        
        print(f"  [OK] Inserted {rows_inserted:,} rows in {elapsed_time:.2f} seconds")
        print()
        
        # Step 2: Validation queries
        print("Step 2: Validating transformed data...")
        
        # Count check
        count_query = text("""
            SELECT 
                COUNT(*) as fact_count,
                COUNT(DISTINCT SaleNumber) as unique_sales,
                SUM(TotalAmount) as total_amount,
                MIN(DateKey) as min_date,
                MAX(DateKey) as max_date
            FROM dbo.fact_sales_transactions
        """)
        
        result = conn.execute(count_query)
        row = result.fetchone()
        
        print(f"  Fact Records: {row[0]:,}")
        print(f"  Unique Sales: {row[1]:,}")
        print(f"  Total Amount: RM {row[2]:,.2f}")
        print(f"  Date Range: {row[3]} to {row[4]}")
        print()
        
        # New fields validation
        new_fields_query = text("""
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN TaxCode IS NOT NULL THEN 1 ELSE 0 END) as tax_code_populated,
                SUM(CASE WHEN IsFOC = 1 THEN 1 ELSE 0 END) as foc_items,
                SUM(CASE WHEN Rounding IS NOT NULL THEN 1 ELSE 0 END) as rounding_populated,
                SUM(CASE WHEN Model IS NOT NULL THEN 1 ELSE 0 END) as model_populated
            FROM dbo.fact_sales_transactions
        """)
        
        result = conn.execute(new_fields_query)
        row = result.fetchone()
        
        print("  New Fields Population:")
        print(f"    Total Rows: {row[0]:,}")
        print(f"    TaxCode Populated: {row[1]:,} ({row[1]/row[0]*100:.1f}%)")
        print(f"    FOC Items: {row[2]:,} ({row[2]/row[0]*100:.1f}%)")
        print(f"    Rounding Populated: {row[3]:,} ({row[3]/row[0]*100:.1f}%)")
        print(f"    Model Populated: {row[4]:,} ({row[4]/row[0]*100:.1f}%)")
        print()
        
        print("="*80)
        print("TRANSFORMATION COMPLETE!")
        print("="*80)
        print()
        print("Next steps:")
        print("  1. Start FastAPI backend with new endpoints")
        print("  2. Access portal at /reports/daily-sales-api-test")
        print("  3. Export to Excel and compare with Xilnex portal")
        print()
    # No need for except/finally - context manager handles cleanup automatically


def transform_to_facts_for_period(start_date: str, end_date: str):
    """
    Transform data from staging tables to fact_sales_transactions for a specific
    BusinessDateTime window [start_date, end_date] (inclusive of dates).

    This function is idempotent for the window: it deletes existing facts in
    the DateKey range before inserting.
    """
    print("="*80)
    print("TRANSFORMING API DATA TO FACT TABLE (WINDOW)")
    print("="*80)
    print(f"Window: {start_date} to {end_date}")
    print()

    engine = get_warehouse_engine()
    
    with engine.begin() as conn:  # Use begin() for automatic transaction management
        try:
            # Compute DateKey bounds
            bounds_query = text("""
                SELECT 
                    CAST(FORMAT(CAST(:start_date AS date), 'yyyyMMdd') AS INT) AS start_key,
                    CAST(FORMAT(CAST(:end_date AS date), 'yyyyMMdd') AS INT) AS end_key
            """)
            start_key, end_key = conn.execute(bounds_query, {"start_date": start_date, "end_date": end_date}).fetchone()

            # Step 0: Remove existing facts in window to keep idempotent
            print("Step 0: Removing existing facts in window...")
            delete_query = text("""
                DELETE FROM dbo.fact_sales_transactions
                WHERE DateKey BETWEEN :start_key AND :end_key
            """)
            del_result = conn.execute(delete_query, {"start_key": start_key, "end_key": end_key})
            print(f"  [OK] Removed {del_result.rowcount if del_result.rowcount is not None else 0} rows")
            print()

            # Step 1: Transform and load for window
            print("Step 1: Transforming and loading data for window...")
            print("  - Applying split-tender allocation logic...")
            print()

            transform_query = text("""
                WITH PaymentAllocations AS (
                    SELECT
                        sp.SaleID,
                        sp.PaymentMethod,
                        sp.CardType,
                        sp.Amount as payment_amount,
                        sp.PaymentReference,
                        sp.EODSessionID,
                        SUM(sp.Amount) OVER (PARTITION BY sp.SaleID) as total_payment_amount,
                        CASE
                            WHEN SUM(sp.Amount) OVER (PARTITION BY sp.SaleID) > 0
                            THEN sp.Amount / SUM(sp.Amount) OVER (PARTITION BY sp.SaleID)
                            ELSE 0
                        END as allocation_percentage
                    FROM dbo.staging_payments sp
                )
                INSERT INTO dbo.fact_sales_transactions (
                    DateKey, TimeKey,
                    LocationKey, ProductKey, CustomerKey, StaffKey,
                    PromotionKey, PaymentTypeKey, TerminalKey,
                    SaleNumber, SaleType, SubSalesType, SalesStatus, OrderSource,
                    Quantity, GrossAmount, DiscountAmount, NetAmount,
                    TaxAmount, TotalAmount, CostAmount, CardType,
                    TaxCode, TaxRate, IsFOC, Rounding, Model, IsServiceCharge
                )
                SELECT
                    CAST(FORMAT(CAST(ss.BusinessDateTime AS DATE), 'yyyyMMdd') AS INT) as DateKey,
                    CAST(REPLACE(CAST(CAST(ss.SystemDateTime AS TIME) AS VARCHAR(8)), ':', '') AS INT) as TimeKey,
                    ISNULL(dl.LocationKey, -1) as LocationKey,
                    ISNULL(dp.ProductKey, -1) as ProductKey,
                    -1 as CustomerKey,
                    ISNULL(ds.StaffKey, -1) as StaffKey,
                    -1 as PromotionKey,
                    ISNULL(dpt.PaymentTypeKey, -1) as PaymentTypeKey,
                    -1 as TerminalKey,
                    CAST(ss.SaleID AS VARCHAR(45)) as SaleNumber,
                    ss.SalesType as SaleType,
                    ss.SubSalesType as SubSalesType,
                    ss.Status as SalesStatus,
                    NULL as OrderSource,
                    si.Quantity,
                    si.Subtotal * pa.allocation_percentage as GrossAmount,
                    si.DiscountAmount * pa.allocation_percentage as DiscountAmount,
                    si.NetAmount * pa.allocation_percentage as NetAmount,
                    si.TaxAmount * pa.allocation_percentage as TaxAmount,
                    si.TotalAmount * pa.allocation_percentage as TotalAmount,
                    si.CostAmount * pa.allocation_percentage as CostAmount, -- CORRECTED: Use CostAmount
                    pa.CardType,
                    si.TaxCode,
                    si.TaxRate,
                    si.IsFOC,
                    ss.Rounding * pa.allocation_percentage as Rounding,
                    si.Model,
                    si.IsServiceCharge
                FROM dbo.staging_sales ss
                JOIN dbo.staging_sales_items si ON ss.SaleID = si.SaleID
                JOIN PaymentAllocations pa ON ss.SaleID = pa.SaleID
                LEFT JOIN dbo.dim_products dp ON si.ProductID = dp.SourceProductID
                LEFT JOIN dbo.dim_locations dl ON ss.OutletName = dl.LocationName
                LEFT JOIN dbo.dim_staff ds ON ss.CashierName = ds.StaffFullName
                LEFT JOIN dbo.dim_payment_types dpt ON pa.PaymentMethod = dpt.PaymentMethodName
                WHERE pa.allocation_percentage > 0
                  AND CAST(ss.BusinessDateTime AS DATE) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
            """)
            ins_result = conn.execute(transform_query, {"start_date": start_date, "end_date": end_date})
            # No need to call conn.commit() - begin() context manager handles it automatically

            print(f"  [OK] Inserted {ins_result.rowcount if ins_result.rowcount is not None else 0:,} rows")
            print()

        except Exception as e:
            print(f"[ERROR] Window transformation failed: {e}")
            traceback.print_exc()
            raise  # Will trigger automatic rollback


def main():
    """Main execution function"""
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║     API DATA TRANSFORMATION TO FACT TABLE                     ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()
    
    transform_to_facts_optimized()
    
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║              TRANSFORMATION COMPLETE!                          ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    main()
