"""
Test inserting a single row to find the exact problem
"""
import os
import json
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv('.env.cloud')

def get_engine():
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER")
    database = os.getenv("TARGET_DATABASE")
    user = os.getenv("TARGET_USERNAME")
    password = quote_plus(os.getenv("TARGET_PASSWORD"))
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    return create_engine(connection_uri, pool_pre_ping=True)

# Load the October 2018 data
print("Loading October 2018 data...")
with open('api_data/raw_sales_October_2018_20251030_123243.json', 'r') as f:
    data = json.load(f)

sales = data['sales']
print(f"Loaded {len(sales)} sales")
print()

# Get first sale
first_sale = sales[0]
print("First sale data:")
sale_id = first_sale.get('id')
print(f"  id: {sale_id} (type={type(sale_id).__name__})")
print(f"  businessDateTime: {first_sale.get('businessDateTime')}")
outlet = first_sale.get('outlet', '')
print(f"  outlet: {outlet} (len={len(str(outlet))})")
cashier = first_sale.get('cashier', '')
print(f"  cashier: {cashier} (len={len(str(cashier))})")
sales_type = first_sale.get('salesType', '')
print(f"  salesType: {sales_type} (len={len(str(sales_type))})")
sub_sales_type = first_sale.get('subSalesType', '')
print(f"  subSalesType: {sub_sales_type} (len={len(str(sub_sales_type))})")
order_no = first_sale.get('orderNo')
print(f"  orderNo: {order_no} (type={type(order_no).__name__}, len={len(str(order_no)) if order_no else 0})")
payment_status = first_sale.get('paymentStatus')
print(f"  paymentStatus: {payment_status} (len={len(str(payment_status)) if payment_status else 0})")
status = first_sale.get('status')
print(f"  status: {status} (len={len(str(status)) if status else 0})")
print()

# Check all string fields
print("All string fields and their lengths:")
for key, value in first_sale.items():
    if isinstance(value, str) and len(value) > 40:
        print(f"  {key}: length={len(value)}, value='{value[:80]}'")
print()

# Try to insert single row
engine = get_engine()
raw_conn = engine.raw_connection()
cursor = raw_conn.cursor()
cursor.fast_executemany = True

insert_sql = """
    INSERT INTO dbo.staging_sales (
        SaleID, BusinessDateTime, SystemDateTime, OutletID, OutletName, CashierName,
        SalesType, SubSalesType, GrandTotal, NetAmount, TaxAmount, Paid, Balance, Rounding,
        PaxNumber, BillDiscountAmount, OrderNo, PaymentStatus, Status, BatchID
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

print("Attempting to insert single row...")
try:
    cursor.execute(insert_sql, (
        first_sale.get('id'),
        first_sale.get('businessDateTime'),
        first_sale.get('dateTime'),
        first_sale.get('outletId'),
        first_sale.get('outlet'),
        first_sale.get('cashier'),
        first_sale.get('salesType'),
        first_sale.get('subSalesType'),
        first_sale.get('grandTotal', 0.0),
        first_sale.get('netAmount', 0.0),
        first_sale.get('gstTaxAmount', 0.0),
        first_sale.get('paid', 0.0),
        first_sale.get('balance', 0.0),
        first_sale.get('rounding', 0.0),
        int(first_sale.get('paxNumber', '0')) if first_sale.get('paxNumber') else None,
        first_sale.get('billDiscountAmount', 0.0),
        first_sale.get('orderNo'),
        first_sale.get('paymentStatus'),
        first_sale.get('status'),
        'TEST123'
    ))
    raw_conn.commit()
    print("[SUCCESS] Single row inserted!")
    
except Exception as e:
    print(f"[ERROR] {e}")
    print(f"Error type: {type(e)}")
    import traceback
    traceback.print_exc()
finally:
    cursor.close()
    raw_conn.close()

