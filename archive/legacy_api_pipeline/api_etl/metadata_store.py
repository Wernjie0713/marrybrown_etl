"""
Persistence helpers for dbo.api_sync_metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text


@dataclass
class MetadataState:
    job_name: str
    last_timestamp: Optional[str]
    status: Optional[str]
    records_extracted: Optional[int]
    date_range_start: Optional[str]
    date_range_end: Optional[str]
    last_chunk_number: Optional[int]
    last_chunk_row_count: Optional[int]
    last_chunk_duration_seconds: Optional[float]
    last_chunk_completed_at: Optional[datetime]


class ApiSyncMetadataStore:
    """
    Handles resume checkpoints stored in dbo.api_sync_metadata.
    """

    def __init__(self, engine_factory):
        self._engine_factory = engine_factory

    def get_state(self, job_name: str) -> Optional[MetadataState]:
        with self._engine_factory().begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT TOP 1
                        JobName,
                        LastTimestamp,
                        Status,
                        RecordsExtracted,
                        DateRangeStart,
                        DateRangeEnd,
                        LastChunkNumber,
                        LastChunkRowCount,
                        LastChunkDurationSeconds,
                        LastChunkCompletedAt
                    FROM dbo.api_sync_metadata
                    WHERE JobName = :job_name
                    ORDER BY SyncID DESC
                    """
                ),
                {"job_name": job_name},
            ).fetchone()

        if not row:
            return None

        return MetadataState(
            job_name=row.JobName,
            last_timestamp=row.LastTimestamp,
            status=row.Status,
            records_extracted=row.RecordsExtracted,
            date_range_start=row.DateRangeStart.isoformat() if row.DateRangeStart else None,
            date_range_end=row.DateRangeEnd.isoformat() if row.DateRangeEnd else None,
            last_chunk_number=row.LastChunkNumber,
            last_chunk_row_count=row.LastChunkRowCount,
            last_chunk_duration_seconds=row.LastChunkDurationSeconds,
            last_chunk_completed_at=row.LastChunkCompletedAt,
        )

    def ensure_job(self, job_name: str, *, start_date: str, end_date: str) -> None:
        """
        Guarantees a metadata row exists for the requested job.
        """
        now = datetime.now(timezone.utc)
        with self._engine_factory().begin() as conn:
            conn.execute(
                text(
                    """
                    MERGE dbo.api_sync_metadata AS target
                    USING (SELECT :job_name AS JobName) AS source
                    ON target.JobName = source.JobName
                    WHEN MATCHED THEN
                        UPDATE SET
                            DateRangeStart = COALESCE(target.DateRangeStart, :date_start),
                            DateRangeEnd = COALESCE(target.DateRangeEnd, :date_end),
                            SyncStartTime = COALESCE(target.SyncStartTime, :sync_start_time)
                    WHEN NOT MATCHED THEN
                        INSERT (
                            JobName,
                            LastTimestamp,
                            SyncStartTime,
                            RecordsExtracted,
                            Status,
                            DateRangeStart,
                            DateRangeEnd
                        )
                        VALUES (
                            :job_name,
                            NULL,
                            :sync_start_time,
                            0,
                            'READY',
                            :date_start,
                            :date_end
                        );
                    """
                ),
                {
                    "job_name": job_name,
                    "date_start": start_date,
                    "date_end": end_date,
                    "sync_start_time": now,
                },
            )

    def update_checkpoint(
        self,
        job_name: str,
        *,
        last_timestamp: Optional[str],
        records_extracted: int,
        status: str,
        date_range_start: str,
        date_range_end: str,
        error_message: Optional[str] = None,
        chunk_number: Optional[int] = None,
        chunk_row_count: Optional[int] = None,
        chunk_duration_seconds: Optional[float] = None,
        chunk_completed_at: Optional[datetime] = None,
    ) -> None:
        """
        Upserts the main checkpoint info plus latest chunk metrics.
        """
        with self._engine_factory().begin() as conn:
            conn.execute(
                text(
                    """
                    MERGE dbo.api_sync_metadata AS target
                    USING (SELECT :job_name AS JobName) AS source
                    ON target.JobName = source.JobName
                    WHEN MATCHED THEN
                        UPDATE SET
                            LastTimestamp = :last_timestamp,
                            RecordsExtracted = :records_extracted,
                            Status = :status,
                            ErrorMessage = :error_message,
                            DateRangeStart = COALESCE(:date_start, target.DateRangeStart),
                            DateRangeEnd = COALESCE(:date_end, target.DateRangeEnd),
                            LastChunkNumber = COALESCE(:chunk_number, target.LastChunkNumber),
                            LastChunkRowCount = COALESCE(:chunk_row_count, target.LastChunkRowCount),
                            LastChunkDurationSeconds = COALESCE(:chunk_duration, target.LastChunkDurationSeconds),
                            LastChunkCompletedAt = COALESCE(:chunk_completed_at, target.LastChunkCompletedAt),
                            SyncEndTime = CASE 
                                WHEN :status IN ('COMPLETED','ERROR') THEN SYSUTCDATETIME()
                                ELSE target.SyncEndTime
                            END
                    WHEN NOT MATCHED THEN
                        INSERT (
                            JobName,
                            LastTimestamp,
                            SyncStartTime,
                            SyncEndTime,
                            RecordsExtracted,
                            Status,
                            ErrorMessage,
                            DateRangeStart,
                            DateRangeEnd,
                            LastChunkNumber,
                            LastChunkRowCount,
                            LastChunkDurationSeconds,
                            LastChunkCompletedAt
                        )
                        VALUES (
                            :job_name,
                            :last_timestamp,
                            SYSUTCDATETIME(),
                            CASE WHEN :status IN ('COMPLETED','ERROR') THEN SYSUTCDATETIME() ELSE NULL END,
                            :records_extracted,
                            :status,
                            :error_message,
                            :date_start,
                            :date_end,
                            :chunk_number,
                            :chunk_row_count,
                            :chunk_duration,
                            :chunk_completed_at
                        );
                    """
                ),
                {
                    "job_name": job_name,
                    "last_timestamp": last_timestamp,
                    "records_extracted": records_extracted,
                    "status": status,
                    "error_message": error_message,
                    "date_start": date_range_start,
                    "date_end": date_range_end,
                    "chunk_number": chunk_number,
                    "chunk_row_count": chunk_row_count,
                    "chunk_duration": chunk_duration_seconds,
                    "chunk_completed_at": chunk_completed_at,
                },
            )


