"""The compute-plane interface: a pool of workers, sized independently of tasks.

A ``Fleet`` is the Snowflake "warehouse" — raw compute capacity. The controller
calls ``ensure(n)`` to bring the worker count up toward demand; idle workers
self-terminate to bring it back down. Crucially, ``n`` is chosen from queue depth /
cost, never from the task count — the same 10 workers can chew through 10 or 10,000
tasks.
"""

from __future__ import annotations


class Fleet:
    def ensure(self, n: int) -> None:
        """Ensure at least ``n`` workers are running (scale up if needed)."""
        ...

    def running(self) -> int:
        """Approximate count of live workers."""
        ...

    def drain(self) -> None:
        """Signal workers to finish current task and stop; block until stopped."""
        ...
