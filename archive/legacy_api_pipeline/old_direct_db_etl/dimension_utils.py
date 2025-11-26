"""
CDC utilities for dimension loaders.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text


def dataframe_hash(df: pd.DataFrame) -> str:
    """
    Deterministic hash leveraging pandas' vectorized hashing.
    """
    if df.empty:
        return "empty"
    hashed = pd.util.hash_pandas_object(df, index=False).values
    return hashlib.sha256(hashed.tobytes()).hexdigest()


@dataclass
class AuditRecord:
    dimension_name: str
    source_hash: str
    row_count: int
    last_run_utc: datetime


class DimensionAuditClient:
    """
    Persists hashes + metadata to dbo.dimension_refresh_audit.
    """

    def __init__(self, engine_factory, dimension_name: str):
        self._engine_factory = engine_factory
        self.dimension_name = dimension_name

    def get_latest(self) -> AuditRecord | None:
        with self._engine_factory().begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT DimensionName, SourceHash, [RowCount], LastRunUTC
                    FROM dbo.dimension_refresh_audit
                    WHERE DimensionName = :name
                    """
                ),
                {"name": self.dimension_name},
            ).fetchone()
        if not row:
            return None
        return AuditRecord(
            dimension_name=row.DimensionName,
            source_hash=row.SourceHash,
            row_count=row.RowCount,
            last_run_utc=row.LastRunUTC,
        )

    def upsert(self, *, source_hash: str, row_count: int, duration_seconds: float) -> None:
        with self._engine_factory().begin() as conn:
            conn.execute(
                text(
                    """
                    MERGE dbo.dimension_refresh_audit AS target
                    USING (SELECT :name AS DimensionName) AS source
                    ON target.DimensionName = source.DimensionName
                    WHEN MATCHED THEN
                        UPDATE SET
                            SourceHash = :hash,
                            [RowCount] = :row_count,
                            LastRunUTC = SYSUTCDATETIME(),
                            LastDurationSeconds = :duration
                    WHEN NOT MATCHED THEN
                        INSERT (DimensionName, SourceHash, [RowCount], LastRunUTC, LastDurationSeconds)
                        VALUES (:name, :hash, :row_count, SYSUTCDATETIME(), :duration);
                    """
                ),
                {
                    "name": self.dimension_name,
                    "hash": source_hash,
                    "row_count": row_count,
                    "duration": duration_seconds,
                },
            )


