"""In-memory leased queue with redelivery + DLQ — the local-backend task plane.

Models the same semantics as SQS so pipeline code behaves identically with or
without AWS: a claimed task is invisible until its lease expires, an expired lease
re-queues the task (incrementing ``attempt``), and a task that exceeds
``max_redeliveries`` is moved to the dead-letter list.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Optional

from ..task import Task
from .base import Queue


class LocalQueue(Queue):
    def __init__(self, max_redeliveries: int = 3):
        self.max_redeliveries = max_redeliveries
        self._lock = threading.Lock()
        self._ready: deque[Task] = deque()
        self._inflight: dict[str, tuple[Task, float]] = {}   # key -> (task, deadline)
        self.dlq: list[Task] = []

    def _sweep(self, now: float) -> None:
        expired = [k for k, (_, dl) in self._inflight.items() if dl <= now]
        for k in expired:
            task, _ = self._inflight.pop(k)
            task.attempt += 1
            if task.attempt > self.max_redeliveries:
                self.dlq.append(task)
            else:
                self._ready.append(task)

    def enqueue(self, task: Task) -> None:
        with self._lock:
            self._ready.append(task)

    def claim(self, lease: float) -> Optional[Task]:
        now = time.time()
        with self._lock:
            self._sweep(now)
            if not self._ready:
                return None
            task = self._ready.popleft()
            self._inflight[task.key] = (task, now + lease)
            return task

    def extend(self, task: Task, lease: float) -> None:
        with self._lock:
            if task.key in self._inflight:
                t, _ = self._inflight[task.key]
                self._inflight[task.key] = (t, time.time() + lease)

    def ack(self, task: Task) -> None:
        with self._lock:
            self._inflight.pop(task.key, None)

    def fail(self, task: Task) -> None:
        with self._lock:
            self._inflight.pop(task.key, None)
            task.attempt += 1
            if task.attempt > self.max_redeliveries:
                self.dlq.append(task)
            else:
                self._ready.append(task)

    def depth(self) -> int:
        with self._lock:
            self._sweep(time.time())
            return len(self._ready) + len(self._inflight)
