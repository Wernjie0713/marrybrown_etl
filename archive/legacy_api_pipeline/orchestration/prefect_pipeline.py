"""
Prefect flow that orchestrates the enhanced Marrybrown ETL.
"""

from __future__ import annotations

import os
from pathlib import Path

from prefect import flow, get_run_logger, task
from utils.env_loader import load_environment

# Load environment - use .env.local for local development
load_environment(force_local=True)


@task(name="refresh_dimensions")
def refresh_dimensions() -> dict:
    """
    Runs the CDC-aware dimension loaders sequentially.
    """
    from direct_db_etl import (
        etl_dim_customers,
        etl_dim_locations,
        etl_dim_payment_types,
        etl_dim_products,
        etl_dim_promotions,
        etl_dim_staff,
        etl_dim_terminals,
    )

    log = get_run_logger()
    tasks = [
        ("dim_products", etl_dim_products.main),
        ("dim_locations", etl_dim_locations.main),
        ("dim_customers", etl_dim_customers.main),
        ("dim_staff", etl_dim_staff.main),
        ("dim_payment_types", etl_dim_payment_types.main),
        ("dim_promotions", etl_dim_promotions.main),
        ("dim_terminals", etl_dim_terminals.main),
    ]

    results = {}
    for name, func in tasks:
        log.info("Refreshing %s ...", name)
        func()
        results[name] = "OK"

    return results


@task(name="run_api_pipeline", retries=1)
def run_api_pipeline(
    start_date: str,
    end_date: str,
    chunk_size: int,
    resume: bool,
) -> dict:
    from api_etl.run_cloud_etl_chunked import run_chunked_etl

    log = get_run_logger()
    log.info(
        "Triggering chunked API ETL for %s -> %s (chunk=%s resume=%s)",
        start_date,
        end_date,
        chunk_size,
        resume,
    )
    run_chunked_etl(
        start_date,
        end_date,
        chunk_size=chunk_size,
        resume=resume,
        enable_early_exit=True,
        buffer_days=7,
        force_restart=not resume,
    )
    return {"status": "completed"}


@task(name="validate_fact_window")
def validate_fact_window(start_date: str, end_date: str) -> None:
    from api_etl.transform_api_to_facts import get_warehouse_engine
    from monitoring import DataQualityValidator

    validator = DataQualityValidator(get_warehouse_engine)
    validator.validate_fact_window(start_date=start_date, end_date=end_date)


@flow(name="marrybrown-etl")
def marrybrown_flow(
    start_date: str = os.getenv("ETL_START_DATE", "2018-10-01"),
    end_date: str = os.getenv("ETL_END_DATE", "2018-12-31"),
    chunk_size: int = int(os.getenv("ETL_CHUNK_SIZE", "50")),
    resume: bool = os.getenv("ETL_RESUME", "true").lower() == "true",
) -> dict:
    """
    Primary Prefect flow; wire this into `prefect deploy`.
    """
    logger = get_run_logger()

    logger.info("Step 1: Refreshing dimensions with CDC checks...")
    dim_result = refresh_dimensions.submit()

    logger.info("Step 2: Running chunked API pipeline...")
    api_result = run_api_pipeline.submit(start_date, end_date, chunk_size, resume)

    logger.info("Step 3: Validating facts vs staging ...")
    quality = validate_fact_window.submit(start_date, end_date)

    return {
        "dimensions": dim_result.result(),
        "api": api_result.result(),
        "quality": "passed" if quality.result() is None else "manual_check",
    }


if __name__ == "__main__":
    marrybrown_flow()


