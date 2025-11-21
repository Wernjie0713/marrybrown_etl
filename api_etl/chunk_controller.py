"""
Adaptive chunk-size controller + latency tracker.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class ChunkTuningConfig:
    min_size: int = 25
    max_size: int = 125
    target_duration_seconds: float = 180.0
    grow_ratio: float = 1.2
    shrink_ratio: float = 0.8
    latency_window: int = 20


class AdaptiveChunkController:
    """
    Tracks per-call latency + chunk flush duration and adjusts chunk size to
    keep checkpoints roughly constant in duration.
    """

    def __init__(self, initial_size: int, config: Optional[ChunkTuningConfig] = None) -> None:
        self.config = config or ChunkTuningConfig()
        self.current_chunk_size = max(self.config.min_size, min(initial_size, self.config.max_size))
        self._latencies: Deque[float] = deque(maxlen=self.config.latency_window)
        self._chunk_start_time: Optional[float] = None

    def record_latency(self, seconds: float) -> None:
        self._latencies.append(seconds)

    def start_chunk_window(self) -> None:
        self._chunk_start_time = time.perf_counter()

    def chunk_duration(self) -> float:
        if not self._chunk_start_time:
            return 0.0
        return time.perf_counter() - self._chunk_start_time

    def reset_chunk_window(self) -> None:
        self._chunk_start_time = time.perf_counter()

    @property
    def moving_average_latency(self) -> Optional[float]:
        if not self._latencies:
            return None
        return sum(self._latencies) / len(self._latencies)

    def adjust_after_flush(self, duration_seconds: float) -> None:
        """
        Shrinks or grows chunk size depending on how long the last chunk took.
        """
        target = self.config.target_duration_seconds
        if duration_seconds > target * 1.25:
            # Chunk took too long -> shrink
            new_size = max(
                self.config.min_size,
                int(self.current_chunk_size * self.config.shrink_ratio),
            )
            if new_size != self.current_chunk_size:
                print(
                    f"[ADAPT] Chunk duration {duration_seconds:.1f}s exceeded target "
                    f"{target:.0f}s. Shrinking chunk size {self.current_chunk_size} -> {new_size}"
                )
            self.current_chunk_size = new_size
        elif duration_seconds < target * 0.75:
            # Chunk was fast -> grow
            new_size = min(
                self.config.max_size,
                int(self.current_chunk_size * self.config.grow_ratio),
            )
            if new_size != self.current_chunk_size:
                print(
                    f"[ADAPT] Chunk duration {duration_seconds:.1f}s below target "
                    f"{target:.0f}s. Growing chunk size {self.current_chunk_size} -> {new_size}"
                )
            self.current_chunk_size = new_size


