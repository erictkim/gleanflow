"""The task-plane interface: a leased, redelivering work queue.

Workers ``claim`` tasks under a time-boxed lease, ``extend`` it while busy, and
``ack`` on success. If a worker dies, its lease expires and the task is redelivered
to another worker — this is what makes one Batch job draining many tasks safe. After
too many redeliveries a task is dead-lettered.

Two implementations: ``LocalQueue`` (in-memory, dev/test) and ``SqsQueue`` (SQS
visibility timeout + redrive policy).
"""

from __future__ import annotations

from typing import Optional

from ..task import Task


class Queue:
    def enqueue(self, task: Task) -> None: ...
    def claim(self, lease: float) -> Optional[Task]:
        """Return a task (leased for ``lease`` seconds) or None if none ready."""
        ...
    def extend(self, task: Task, lease: float) -> None: ...
    def ack(self, task: Task) -> None: ...
    def fail(self, task: Task) -> None:
        """Negative-ack: redeliver promptly (or dead-letter if exhausted)."""
        ...
    def depth(self) -> int:
        """Approximate count of pending + in-flight tasks."""
        ...
