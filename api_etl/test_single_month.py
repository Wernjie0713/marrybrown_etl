"""
Test single month extraction with debugging
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from extract_from_api import extract_sales_for_period, load_to_staging

print("Testing October 2018 extraction...")
print()

# Extract October 2018
sales = extract_sales_for_period('2018-10-01', '2018-10-31')
print(f"Extracted {len(sales)} sales")
print()

# Try to load
print("Attempting to load to staging...")
try:
    load_to_staging(sales)
    print("\n[SUCCESS] Load completed!")
except Exception as e:
    print(f"\n[FAILED] Error: {e}")
    import traceback
    traceback.print_exc()

