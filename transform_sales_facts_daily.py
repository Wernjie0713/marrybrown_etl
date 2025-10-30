"""
Daily Transformation Script for Fact Sales Transactions
Processes one day at a time to avoid timeout issues
Loads data from staging tables to fact_sales_transactions with split-tender allocation

CRITICAL LOGIC:
- Returns: Use actual amounts WITHOUT payment allocation (returns represent refunds, not payment splits)
- Normal Sales WITH payments: Use payment allocation for split-tender scenarios
- Normal Sales WITHOUT payments: Use full amounts (rare edge case)
"""

import os
from datetime import date, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import time

# Configuration
START_DATE = date(2025, 9, 1)
END_DATE = date(2025, 9, 30)

def get_db_engine(prefix="TARGET"):
    """Creates a SQLAlchemy engine for the target warehouse."""
    driver = os.getenv(f"{prefix}_DRIVER").replace(" ", "+")
    server = os.getenv(f"{prefix}_SERVER")
    database = os.getenv(f"{prefix}_DATABASE")
    user = os.getenv(f"{prefix}_USERNAME")
    password = os.getenv(f"{prefix}_PASSWORD")
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    
    return create_engine(connection_uri, pool_pre_ping=True)

def transform_single_day(engine, process_date):
    """Transform data for a single day using split-tender allocation logic."""
    
    date_key = int(process_date.strftime('%Y%m%d'))
    date_str = process_date.strftime('%Y-%m-%d')
    
    print(f"  Processing {date_str} (DateKey: {date_key})...")
    start_time = time.time()
    
    with engine.connect() as conn:
        # Step 1: Delete existing data for this date
        print(f"    - Deleting existing fact data for {date_str}...")
        delete_query = text("""
            DELETE FROM dbo.fact_sales_transactions
            WHERE DateKey = :date_key
        """)
        result = conn.execute(delete_query, {"date_key": date_key})
        deleted_rows = result.rowcount
        print(f"    - Deleted {deleted_rows} existing rows")
        
        # Step 2: Insert new data with split-tender allocation
        print(f"    - Inserting transformed data for {date_str}...")
        
        insert_query = text("""
            -- NEW APPROACH: Handle split-tender payments by allocating amounts proportionally
            WITH InvoicePaymentTotals AS (
                -- Calculate total payment amount per invoice (FOR THIS DATE ONLY)
                SELECT
                    invoice_id,
                    SUM(double_amount) as total_payment_amount
                FROM dbo.staging_payments
                WHERE DATETIME__DATE = :process_date
                GROUP BY invoice_id
            ),
            PaymentAllocations AS (
                -- Calculate allocation percentage for each payment method per invoice
                SELECT
                    sp.invoice_id,
                    sp.method,
                    sp.card_type,
                    sp.double_amount as payment_amount,
                    ipt.total_payment_amount,
                    -- Calculate what percentage of total payment this method represents
                    CASE
                        WHEN ipt.total_payment_amount > 0
                        THEN sp.double_amount / ipt.total_payment_amount
                        ELSE 0
                    END as allocation_percentage
                FROM dbo.staging_payments sp
                JOIN InvoicePaymentTotals ipt ON sp.invoice_id = ipt.invoice_id
                WHERE sp.DATETIME__DATE = :process_date
            ),
            RecipeCalculatedCosts AS (
                -- Use pre-calculated recipe costs from staging table
                -- This staging table is populated during extraction from COM_5013.APP_4_RECIPESUMMARY
                SELECT
                    item_id,
                    sales_type,
                    calculated_recipe_cost
                FROM dbo.staging_recipe_costs
            ),
            LineItemsWithPayments AS (
                -- BRANCH 1: RETURN transactions - use actual amounts WITHOUT payment allocation
                -- Returns represent money being refunded, not split across payment methods
                SELECT
                    -- From sales header (ss)
                    ss.sales_no,
                    ss.datetime__sales_date,
                    ss.now_time,
                    ss.sale_location,
                    ss.CUSTOMER_ID,
                    ss.CASHIER,
                    ss.SALES_PERSON,
                    ss.SALES_PERSON_USERNAME,
                    ss.SALES_TYPE,
                    ss.SUBSALES_TYPE,
                    ss.ORDER_SOURCE,
                    ss.terminal_id,
                    ss.SALES_STATUS,
                    -- From sales items (ssi)
                    ssi.item_code,
                    ssi.INT_QUANTITY,
                    ssi.voucher_no,
                    ssi.double_cost,
                    -- No payment allocation for returns
                    NULL as method,
                    NULL as card_type,
                    NULL as payment_amount,
                    NULL as total_payment_amount,
                    NULL as allocation_percentage,
                    -- Use actual return amounts (typically negative)
                    (ssi.double_sub_total - ssi.double_total_tax_amount) AS allocated_gross_amount,
                    ssi.double_total_discount_amount AS allocated_discount_amount,
                    (ssi.double_sub_total - ssi.double_total_tax_amount) AS allocated_net_amount,
                    ssi.double_total_tax_amount AS allocated_tax_amount,
                    ssi.double_sub_total AS allocated_total_amount,
                    -- Use recipe-calculated cost for combo items, fallback to source cost
                    (COALESCE(rcc.calculated_recipe_cost, ssi.double_cost) * ssi.INT_QUANTITY) AS allocated_cost_amount
                FROM dbo.staging_sales_items ssi
                JOIN dbo.staging_sales ss ON ssi.sales_no = ss.sales_no
                LEFT JOIN RecipeCalculatedCosts rcc 
                    ON TRY_CAST(ssi.item_code AS BIGINT) = rcc.item_id
                    AND rcc.SALES_TYPE = CASE 
                        WHEN ss.SALES_TYPE = 'Dine In' THEN '1'
                        WHEN ss.SALES_TYPE = 'Take Away' THEN '2'
                        WHEN ss.SALES_TYPE = 'Delivery' THEN '3'
                        WHEN ss.SALES_TYPE = 'Drive Thru' THEN '4'
                        ELSE '1'
                    END
                WHERE ss.datetime__sales_date = :process_date
                  AND ss.SALES_TYPE = 'Return'  -- Only return transactions

                UNION ALL

                -- BRANCH 2: Normal sales WITH payments - use payment allocation for split-tender
                SELECT
                    -- From sales header (ss)
                    ss.sales_no,
                    ss.datetime__sales_date,
                    ss.now_time,
                    ss.sale_location,
                    ss.CUSTOMER_ID,
                    ss.CASHIER,
                    ss.SALES_PERSON,
                    ss.SALES_PERSON_USERNAME,
                    ss.SALES_TYPE,
                    ss.SUBSALES_TYPE,
                    ss.ORDER_SOURCE,
                    ss.terminal_id,
                    ss.SALES_STATUS,
                    -- From sales items (ssi)
                    ssi.item_code,
                    ssi.INT_QUANTITY,
                    ssi.voucher_no,
                    ssi.double_cost,
                    -- From payment allocations (pa)
                    pa.method,
                    pa.card_type,
                    pa.payment_amount,
                    pa.total_payment_amount,
                    pa.allocation_percentage,
                    -- Allocate line item amounts proportionally to each payment method
                    (ssi.double_sub_total - ssi.double_total_tax_amount) * pa.allocation_percentage AS allocated_gross_amount,
                    ssi.double_total_discount_amount * pa.allocation_percentage AS allocated_discount_amount,
                    (ssi.double_sub_total - ssi.double_total_tax_amount) * pa.allocation_percentage AS allocated_net_amount,
                    ssi.double_total_tax_amount * pa.allocation_percentage AS allocated_tax_amount,
                    ssi.double_sub_total * pa.allocation_percentage AS allocated_total_amount,
                    -- Use recipe-calculated cost for combo items, fallback to source cost, then allocate
                    (COALESCE(rcc.calculated_recipe_cost, ssi.double_cost) * ssi.INT_QUANTITY) * pa.allocation_percentage AS allocated_cost_amount
                FROM dbo.staging_sales_items ssi
                JOIN dbo.staging_sales ss ON ssi.sales_no = ss.sales_no
                LEFT JOIN PaymentAllocations pa ON pa.invoice_id = ss.sales_no
                LEFT JOIN RecipeCalculatedCosts rcc 
                    ON TRY_CAST(ssi.item_code AS BIGINT) = rcc.item_id
                    AND rcc.SALES_TYPE = CASE 
                        WHEN ss.SALES_TYPE = 'Dine In' THEN '1'
                        WHEN ss.SALES_TYPE = 'Take Away' THEN '2'
                        WHEN ss.SALES_TYPE = 'Delivery' THEN '3'
                        WHEN ss.SALES_TYPE = 'Drive Thru' THEN '4'
                        ELSE '1'
                    END
                WHERE ss.datetime__sales_date = :process_date
                  AND ss.SALES_TYPE != 'Return'  -- Exclude returns (handled in branch 1)
                  AND pa.allocation_percentage IS NOT NULL  -- Only include items with payment allocation

                UNION ALL

                -- BRANCH 3: Normal sales WITHOUT payment records (use full amounts, PaymentTypeKey will be -1)
                SELECT
                    -- From sales header (ss)
                    ss.sales_no,
                    ss.datetime__sales_date,
                    ss.now_time,
                    ss.sale_location,
                    ss.CUSTOMER_ID,
                    ss.CASHIER,
                    ss.SALES_PERSON,
                    ss.SALES_PERSON_USERNAME,
                    ss.SALES_TYPE,
                    ss.SUBSALES_TYPE,
                    ss.ORDER_SOURCE,
                    ss.terminal_id,
                    ss.SALES_STATUS,
                    -- From sales items (ssi)
                    ssi.item_code,
                    ssi.INT_QUANTITY,
                    ssi.voucher_no,
                    ssi.double_cost,
                    -- No payment allocation
                    NULL as method,
                    NULL as card_type,
                    NULL as payment_amount,
                    NULL as total_payment_amount,
                    NULL as allocation_percentage,
                    -- Use full amounts (no allocation)
                    (ssi.double_sub_total - ssi.double_total_tax_amount) AS allocated_gross_amount,
                    ssi.double_total_discount_amount AS allocated_discount_amount,
                    (ssi.double_sub_total - ssi.double_total_tax_amount) AS allocated_net_amount,
                    ssi.double_total_tax_amount AS allocated_tax_amount,
                    ssi.double_sub_total AS allocated_total_amount,
                    -- Use recipe-calculated cost for combo items, fallback to source cost
                    (COALESCE(rcc.calculated_recipe_cost, ssi.double_cost) * ssi.INT_QUANTITY) AS allocated_cost_amount
                FROM dbo.staging_sales_items ssi
                JOIN dbo.staging_sales ss ON ssi.sales_no = ss.sales_no
                LEFT JOIN RecipeCalculatedCosts rcc 
                    ON TRY_CAST(ssi.item_code AS BIGINT) = rcc.item_id
                    AND rcc.SALES_TYPE = CASE 
                        WHEN ss.SALES_TYPE = 'Dine In' THEN '1'
                        WHEN ss.SALES_TYPE = 'Take Away' THEN '2'
                        WHEN ss.SALES_TYPE = 'Delivery' THEN '3'
                        WHEN ss.SALES_TYPE = 'Drive Thru' THEN '4'
                        ELSE '1'
                    END
                WHERE ss.datetime__sales_date = :process_date
                  AND ss.SALES_TYPE != 'Return'  -- Exclude returns (handled in branch 1)
                  AND NOT EXISTS (
                    SELECT 1
                    FROM dbo.staging_payments sp
                    WHERE sp.invoice_id = ss.sales_no
                )
            )
            INSERT INTO dbo.fact_sales_transactions (
                -- Keys
                DateKey,
                TimeKey,
                LocationKey,
                ProductKey,
                CustomerKey,
                StaffKey,
                PromotionKey,
                PaymentTypeKey,
                TerminalKey,
                -- Degenerate Dimensions
                SaleNumber,
                SaleType,
                OrderSource,
                SalesStatus,
                SubSalesType,
                -- Measures
                Quantity,
                GrossAmount,
                DiscountAmount,
                NetAmount,
                TaxAmount,
                TotalAmount,
                CostAmount,
                CardType
            )
            SELECT
                -- Keys: Join to dimensions to get the surrogate keys.
                COALESCE(d.DateKey, -1) AS DateKey,
                COALESCE(t.TimeKey, -1) AS TimeKey,
                COALESCE(l.LocationKey, -1) AS LocationKey,
                COALESCE(p.ProductKey, -1) AS ProductKey,
                COALESCE(c.CustomerKey, -1) AS CustomerKey,
                COALESCE(st.StaffKey, -1) AS StaffKey,
                COALESCE(pr.PromotionKey, -1) AS PromotionKey,
                COALESCE(pt.PaymentTypeKey, -1) AS PaymentTypeKey,
                COALESCE(term.TerminalKey, -1) AS TerminalKey,

                -- Degenerate Dimensions
                liwp.sales_no AS SaleNumber,
                liwp.SALES_TYPE AS SaleType,
                liwp.ORDER_SOURCE AS OrderSource,
                liwp.SALES_STATUS AS SalesStatus,
                liwp.SUBSALES_TYPE AS SubSalesType,

                -- Measures: Use allocated amounts
                liwp.INT_QUANTITY AS Quantity,
                liwp.allocated_gross_amount AS GrossAmount,
                liwp.allocated_discount_amount AS DiscountAmount,
                liwp.allocated_net_amount AS NetAmount,
                liwp.allocated_tax_amount AS TaxAmount,
                liwp.allocated_total_amount AS TotalAmount,
                liwp.allocated_cost_amount AS CostAmount,
                liwp.card_type AS CardType

            FROM
                LineItemsWithPayments AS liwp
            LEFT JOIN
                dbo.dim_date AS d ON d.DateKey = CONVERT(INT, CONVERT(VARCHAR(8), liwp.datetime__sales_date, 112))
            LEFT JOIN
                dbo.dim_time AS t ON t.TimeKey = CONVERT(INT, REPLACE(CONVERT(VARCHAR(8), liwp.now_time, 108), ':', ''))
            LEFT JOIN
                dbo.dim_locations AS l ON l.LocationGUID = liwp.sale_location
            LEFT JOIN
                dbo.dim_terminals AS term
                    ON term.TerminalID = liwp.terminal_id
                    AND term.LocationKey = COALESCE(l.LocationKey, -1)
            LEFT JOIN
                dbo.dim_products AS p ON TRY_CAST(liwp.item_code AS BIGINT) = p.SourceProductID
            LEFT JOIN
                dbo.dim_customers AS c ON c.CustomerGUID = liwp.CUSTOMER_ID
            LEFT JOIN
                dbo.dim_staff AS st ON st.StaffUsername = COALESCE(liwp.SALES_PERSON_USERNAME, liwp.CASHIER)
            LEFT JOIN
                dbo.dim_promotions AS pr ON pr.PromotionCode = liwp.voucher_no
            LEFT JOIN
                dbo.dim_payment_types AS pt ON pt.PaymentMethodName = liwp.method
        """)
        
        result = conn.execute(insert_query, {"process_date": date_str})
        inserted_rows = result.rowcount
        conn.commit()
        
        elapsed = time.time() - start_time
        print(f"    - Inserted {inserted_rows} new rows")
        print(f"    [OK] Completed in {elapsed:.2f} seconds")
        
        return inserted_rows

def main():
    print("=" * 80)
    print("DAY-BY-DAY TRANSFORMATION: FACT SALES TRANSACTIONS")
    print("=" * 80)
    print(f"\nDate Range: {START_DATE} to {END_DATE}")
    
    # Calculate number of days
    days_to_process = (END_DATE - START_DATE).days + 1
    print(f"Total Days: {days_to_process}")
    print("\nLoading data from staging tables with split-tender payment allocation...")
    print("=" * 80)
    
    # Load environment variables
    load_dotenv()
    
    # Create database engine
    print("\nConnecting to target warehouse...")
    engine = get_db_engine("TARGET")
    
    # Process each day
    total_inserted = 0
    current_date = START_DATE
    day_num = 0
    
    overall_start = time.time()
    
    while current_date <= END_DATE:
        day_num += 1
        print(f"\n[Day {day_num}/{days_to_process}]")
        
        try:
            rows_inserted = transform_single_day(engine, current_date)
            total_inserted += rows_inserted
        except Exception as e:
            print(f"    [ERROR]: {e}")
            print(f"    Skipping {current_date} and continuing...")
        
        current_date += timedelta(days=1)
    
    overall_elapsed = time.time() - overall_start
    
    print("\n" + "=" * 80)
    print("TRANSFORMATION COMPLETE")
    print("=" * 80)
    print(f"Total Days Processed: {day_num}")
    print(f"Total Rows Inserted: {total_inserted:,}")
    print(f"Total Time: {overall_elapsed:.2f} seconds ({overall_elapsed/60:.2f} minutes)")
    print(f"Average per Day: {overall_elapsed/day_num:.2f} seconds")
    print("\n[SUCCESS] Ready for Phase 2 validation!")
    print("=" * 80)

if __name__ == "__main__":
    main()

