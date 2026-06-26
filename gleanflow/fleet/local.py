"""Local fleet — N worker threads draining the in-memory queue (no AWS).

Proves the whole three-plane model on a laptop: ``ensure(n)`` tops the live thread
count up to ``n``, each thread runs the same ``poll_loop`` a Batch worker runs, and
threads exit on idle just like Batch jobs do. ``workers=1`` vs ``workers=8`` over the
same task set yields identical output — the decoupling guarantee, testable offline.
"""

from __future__ import annotations

import threading

from .. import worker as worker_mod
from .base import Fleet


class LocalFleet(Fleet):
    def __init__(self, *, queue, store, pipe, tracker=None, idle_timeout=5.0,
                 lease=300.0, heartbeat=30.0, force=False):
        self.queue = queue
        self.store = store
        self.pipe = pipe
        self.tracker = tracker
        self.idle_timeout = idle_timeout
        self.lease = lease
        self.heartbeat = heartbeat
        self.force = force
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._n = 0

    def _spawn(self, idx: int) -> threading.Thread:
        t = threading.Thread(
            target=worker_mod.poll_loop,
            kwargs=dict(queue=self.queue, store=self.store, pipe=self.pipe,
                        tracker=self.tracker, worker_id=f"w{idx}",
                        idle_timeout=self.idle_timeout, lease=self.lease,
                        heartbeat=self.heartbeat, force=self.force, stop=self._stop),
            daemon=True,
        )
        t.start()
        return t

    def ensure(self, n: int) -> None:
        while self.running() < n:
            self._threads.append(self._spawn(self._n))
            self._n += 1

    def running(self) -> int:
        return sum(t.is_alive() for t in self._threads)

    def drain(self) -> None:
        # let workers finish via idle-timeout; join them
        for t in self._threads:
            t.join(timeout=self.idle_timeout + 10)

    def stop_now(self) -> None:
        self._stop.set()
        self.drain()
