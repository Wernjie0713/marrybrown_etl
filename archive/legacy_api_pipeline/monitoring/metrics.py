"""
Structured metrics emission for the ETL pipeline.

Primary goals:
- Emit JSON log lines for every significant checkpoint (chunk save, retries, etc.)
- Optionally push metrics to a Prometheus Pushgateway when configured
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

try:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
except Exception:  # pragma: no cover - dependency optional during local dev
    CollectorRegistry = None
    Gauge = None
    push_to_gateway = None


@dataclass
class MetricTags:
    """Key/value tags shared across emissions."""

    job_name: str
    environment: str = os.getenv("ETL_ENVIRONMENT", "dev")


class MetricsEmitter:
    """
    Emits structured metrics locally and (optionally) to Prometheus Pushgateway.
    """

    def __init__(self, tags: MetricTags | None = None) -> None:
        self.tags = tags or MetricTags(job_name="marrybrown_etl")
        log_path = os.getenv("METRICS_LOG_PATH", "monitoring/metrics.log")
        self._log_path = Path(log_path)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._prom_gateway = os.getenv("PROM_PUSHGATEWAY_URL")
        self._prom_job = os.getenv("PROM_PUSH_JOB_NAME", self.tags.job_name)

    def emit(
        self,
        metric_name: str,
        payload: Dict,
        *,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Write JSON line and optionally push to Prometheus."""
        event_time = timestamp or datetime.now(timezone.utc)
        record = {
            "metric": metric_name,
            "timestamp": event_time.isoformat(),
            "tags": self.tags.__dict__,
            "payload": payload,
        }
        with self._lock:
            with self._log_path.open("a", encoding="utf-8") as log_file:
                log_file.write(json.dumps(record, default=str) + os.linesep)

        self._maybe_push_to_prometheus(metric_name, payload, event_time)

    # ------------------------------------------------------------------ #
    # Convenience helpers
    # ------------------------------------------------------------------ #
    def emit_chunk_metrics(
        self,
        *,
        chunk_number: int,
        duration_seconds: float,
        row_count: int,
        api_calls_in_chunk: int,
        last_timestamp: Optional[str],
        retries: int,
    ) -> None:
        self.emit(
            "etl_chunk_checkpoint",
            {
                "chunk_number": chunk_number,
                "duration_seconds": duration_seconds,
                "row_count": row_count,
                "api_calls": api_calls_in_chunk,
                "last_timestamp": last_timestamp,
                "retries": retries,
            },
        )

    def emit_retry_event(self, *, attempt: int, wait_seconds: float, reason: str) -> None:
        self.emit(
            "etl_retry",
            {
                "attempt": attempt,
                "wait_seconds": wait_seconds,
                "reason": reason,
            },
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _maybe_push_to_prometheus(
        self, metric_name: str, payload: Dict, event_time: datetime
    ) -> None:
        if not self._prom_gateway or not push_to_gateway or not CollectorRegistry:
            return

        registry = CollectorRegistry()
        labels = {"job_name": self.tags.job_name, "metric": metric_name}

        if metric_name == "etl_chunk_checkpoint":
            duration = payload.get("duration_seconds", 0)
            rows = payload.get("row_count", 0)
            chunk = payload.get("chunk_number", 0)

            duration_gauge = Gauge(
                "etl_chunk_duration_seconds",
                "Chunk duration in seconds",
                labelnames=list(labels.keys()),
                registry=registry,
            )
            duration_gauge.labels(**labels).set(duration)

            rows_gauge = Gauge(
                "etl_chunk_rows_total",
                "Rows processed in chunk",
                labelnames=list(labels.keys()),
                registry=registry,
            )
            rows_gauge.labels(**labels).set(rows)

            chunk_gauge = Gauge(
                "etl_chunk_number",
                "Latest completed chunk number",
                labelnames=list(labels.keys()),
                registry=registry,
            )
            chunk_gauge.labels(**labels).set(chunk)

        elif metric_name == "etl_retry":
            wait = payload.get("wait_seconds", 0)
            attempt = payload.get("attempt", 1)

            wait_gauge = Gauge(
                "etl_retry_wait_seconds",
                "Retry wait duration",
                labelnames=list(labels.keys()),
                registry=registry,
            )
            wait_gauge.labels(**labels).set(wait)

            attempt_gauge = Gauge(
                "etl_retry_attempt",
                "Retry attempt count",
                labelnames=list(labels.keys()),
                registry=registry,
            )
            attempt_gauge.labels(**labels).set(attempt)

        push_to_gateway(
            self._prom_gateway,
            job=self._prom_job,
            registry=registry,
            grouping_key={"emitted_at": event_time.isoformat()},
        )


