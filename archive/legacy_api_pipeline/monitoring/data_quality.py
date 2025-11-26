"""
Shared data-quality validations for API + direct DB ETL pipelines.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from sqlalchemy import text


class DataQualityError(RuntimeError):
    """Raised when a data-quality gate fails."""


@dataclass
class ChunkQualityResult:
    total_sales_in_window: int
    distinct_sales_in_window: int
    violations: Dict[str, str]


class DataQualityValidator:
    """
    Runs lightweight SQL checks to prevent bad data from flowing downstream.
    """

    def __init__(self, engine_factory):
        self._engine_factory = engine_factory

    # ------------------------------------------------------------------ #
    # API chunk checks
    # ------------------------------------------------------------------ #
    def validate_staging_chunk(
        self,
        *,
        start_date: str,
        end_date: str,
        expected_sales: int,
    ) -> ChunkQualityResult:
        """
        Ensures staging counts and uniqueness look sane for the target window.
        """
        with self._engine_factory().begin() as conn:
            summary = conn.execute(
                text(
                    """
                    SELECT 
                        COUNT(*) AS total_rows,
                        COUNT(DISTINCT SaleID) AS distinct_sales
                    FROM dbo.staging_sales
                    WHERE CAST(BusinessDateTime AS DATE) 
                          BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
                    """
                ),
                {"start_date": start_date, "end_date": end_date},
            ).fetchone()

        total_rows = summary.total_rows or 0
        distinct_sales = summary.distinct_sales or 0

        violations: Dict[str, str] = {}

        if expected_sales and total_rows < expected_sales * 0.9:
            violations["row_shortfall"] = (
                f"Expected >= {expected_sales}, found {total_rows} rows in staging_sales"
            )

        if distinct_sales < total_rows:
            violations["duplicates"] = (
                f"{total_rows - distinct_sales} duplicate SaleID rows detected"
            )

        return ChunkQualityResult(
            total_sales_in_window=total_rows,
            distinct_sales_in_window=distinct_sales,
            violations=violations,
        )

    # ------------------------------------------------------------------ #
    # Fact-table checks
    # ------------------------------------------------------------------ #
    def validate_fact_window(self, *, start_date: str, end_date: str) -> None:
        """
        Compare staging vs fact counts and revenue totals to catch drift early.
        """
        with self._engine_factory().begin() as conn:
            staging_row = conn.execute(
                text(
                    """
                    SELECT 
                        COUNT(*) AS item_rows,
                        SUM(CAST(si.TotalAmount AS DECIMAL(18,2))) AS total_amount,
                        SUM(CAST(si.NetAmount AS DECIMAL(18,2))) AS net_amount,
                        SUM(CAST(si.TaxAmount AS DECIMAL(18,2))) AS tax_amount,
                        SUM(CAST(si.CostAmount AS DECIMAL(18,2))) AS cost_amount
                    FROM dbo.staging_sales_items si
                    JOIN dbo.staging_sales ss ON ss.SaleID = si.SaleID
                    WHERE CAST(ss.BusinessDateTime AS DATE) 
                          BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
                    """
                ),
                {"start_date": start_date, "end_date": end_date},
            ).fetchone()

            fact_row = conn.execute(
                text(
                    """
                    SELECT 
                        COUNT(*) AS fact_rows,
                        SUM(CAST(TotalAmount AS DECIMAL(18,2))) AS fact_amount,
                        SUM(CAST(NetAmount AS DECIMAL(18,2))) AS fact_net,
                        SUM(CAST(TaxAmount AS DECIMAL(18,2))) AS fact_tax,
                        SUM(CAST(CostAmount AS DECIMAL(18,2))) AS fact_cost
                    FROM dbo.fact_sales_transactions
                    WHERE DATEFROMPARTS(
                        DateKey / 10000,
                        (DateKey / 100) % 100,
                        DateKey % 100
                    ) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
                    """
                ),
                {"start_date": start_date, "end_date": end_date},
            ).fetchone()

        stage_amount = staging_row.total_amount or 0
        fact_amount = fact_row.fact_amount or 0
        
        stage_net = staging_row.net_amount or 0
        fact_net = fact_row.fact_net or 0
        
        stage_tax = staging_row.tax_amount or 0
        fact_tax = fact_row.fact_tax or 0
        
        stage_cost = staging_row.cost_amount or 0
        fact_cost = fact_row.fact_cost or 0

        stage_rows = staging_row.item_rows or 0
        fact_rows = fact_row.fact_rows or 0

        if fact_rows == 0:
            # This might be valid if staging is empty, but staging check covers that.
            # If staging has rows, fact must have rows.
            pass

        if stage_rows == 0:
            raise DataQualityError("Staging rows missing for requested window.")
            
        if fact_rows == 0 and stage_rows > 0:
             raise DataQualityError("Fact table returned 0 rows but staging has data.")

        # Validate Total Amount
        diff = abs(stage_amount - fact_amount)
        if stage_amount and (diff / stage_amount) > 0.01:
            raise DataQualityError(
                f"Fact TotalAmount differs from staging by {diff:.2f} (>{1:.0f}% threshold)."
            )
            
        # Validate Net Amount
        diff_net = abs(stage_net - fact_net)
        if stage_net and (diff_net / stage_net) > 0.01:
            raise DataQualityError(
                 f"Fact NetAmount differs from staging by {diff_net:.2f}."
            )

        # Validate Tax Amount
        diff_tax = abs(stage_tax - fact_tax)
        if stage_tax and (diff_tax / stage_tax) > 0.01:
             raise DataQualityError(
                 f"Fact TaxAmount differs from staging by {diff_tax:.2f}."
             )
             
        # Validate Cost Amount
        diff_cost = abs(stage_cost - fact_cost)
        if stage_cost and (diff_cost / stage_cost) > 0.01:
             raise DataQualityError(
                 f"Fact CostAmount differs from staging by {diff_cost:.2f}."
             )


