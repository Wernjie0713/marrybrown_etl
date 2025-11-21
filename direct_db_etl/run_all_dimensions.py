"""
Run all dimension ETL scripts in sequence.
This script executes all dimension table ETL processes.
"""

import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.env_loader import load_environment

# Load environment variables
load_environment(force_local=True)

# Import all dimension ETL modules
from direct_db_etl.etl_dim_locations import main as etl_dim_locations
from direct_db_etl.etl_dim_products import main as etl_dim_products
from direct_db_etl.etl_dim_customers import main as etl_dim_customers
from direct_db_etl.etl_dim_staff import main as etl_dim_staff
from direct_db_etl.etl_dim_payment_types import main as etl_dim_payment_types
from direct_db_etl.etl_dim_promotions import main as etl_dim_promotions
from direct_db_etl.etl_dim_terminals import main as etl_dim_terminals


def run_all_dimensions():
    """Run all dimension ETL scripts in sequence."""
    
    # Define the order of execution
    # Note: Order matters if there are dependencies between dimensions
    dimension_scripts = [
        ("dim_locations", etl_dim_locations),
        ("dim_products", etl_dim_products),
        ("dim_customers", etl_dim_customers),
        ("dim_staff", etl_dim_staff),
        ("dim_payment_types", etl_dim_payment_types),
        ("dim_promotions", etl_dim_promotions),
        ("dim_terminals", etl_dim_terminals),
    ]
    
    print("=" * 80)
    print("Starting ETL for ALL Dimension Tables")
    print("=" * 80)
    print()
    
    total_start_time = time.perf_counter()
    results = []
    
    for dim_name, etl_function in dimension_scripts:
        print(f"\n{'=' * 80}")
        print(f"Processing: {dim_name}")
        print(f"{'=' * 80}")
        
        try:
            start_time = time.perf_counter()
            etl_function()
            elapsed = time.perf_counter() - start_time
            results.append((dim_name, "SUCCESS", elapsed))
            print(f"[OK] {dim_name} completed in {elapsed:.2f}s")
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            results.append((dim_name, "FAILED", elapsed))
            print(f"[ERROR] {dim_name} failed after {elapsed:.2f}s: {e}")
            # Continue with next dimension even if one fails
            continue
    
    total_elapsed = time.perf_counter() - total_start_time
    
    # Print summary
    print("\n" + "=" * 80)
    print("ETL Summary")
    print("=" * 80)
    print(f"{'Dimension':<25} {'Status':<10} {'Duration':<15}")
    print("-" * 80)
    
    for dim_name, status, elapsed in results:
        status_symbol = "[OK]" if status == "SUCCESS" else "[ERROR]"
        print(f"{dim_name:<25} {status_symbol} {status:<7} {elapsed:>8.2f}s")
    
    print("-" * 80)
    print(f"{'Total Time':<25} {'':<10} {total_elapsed:>8.2f}s")
    print("=" * 80)
    
    # Count successes and failures
    successes = sum(1 for _, status, _ in results if status == "SUCCESS")
    failures = sum(1 for _, status, _ in results if status == "FAILED")
    
    print(f"\nCompleted: {successes}/{len(results)} dimensions")
    if failures > 0:
        print(f"Failed: {failures} dimensions")
        sys.exit(1)
    else:
        print("[OK] All dimension ETL processes completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    run_all_dimensions()

