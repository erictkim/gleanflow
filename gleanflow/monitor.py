"""In-container CPU/memory sampler (no third-party deps; reads cgroup files).

A daemon thread samples the container's cgroup memory + CPU and tracks the peak.
The peak is folded into the task's completion marker so the controller can report
OOM margin and (stretch) auto-bump memory on retry. Falls back to ``resource``
RSS when cgroup files are absent (e.g. running locally on macOS).
"""

from __future__ import annotations

import threading
import time
from typing import Optional


class Monitor:
    def __init__(self, interval: float = 20.0):
        self.interval = interval
        self.peak_mem = 0
        self.limit = 0
        self._cpu0 = _cpu_usec()
        self._t0 = time.time()
        self._stop = threading.Event()
        self._thr: Optional[threading.Thread] = None

    def start(self) -> "Monitor":
        self._sample()
        self._thr = threading.Thread(target=self._loop, daemon=True)
        self._thr.start()
        return self

    def _loop(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def _sample(self):
        cur, peak, limit = _mem()
        self.peak_mem = max(self.peak_mem, peak or cur)
        self.limit = limit

    def stop(self) -> dict:
        self._stop.set()
        self._sample()
        cpu = (_cpu_usec() - self._cpu0) / 1e6
        return {
            "peak_mem_mb": round(self.peak_mem / 1e6, 1) if self.peak_mem else None,
            "limit_mb": round(self.limit / 1e6, 1) if self.limit else None,
            "cpu_seconds": round(cpu, 1) if cpu >= 0 else None,
            "wall_seconds": round(time.time() - self._t0, 1),
        }


def _read_int(path: str) -> Optional[int]:
    try:
        with open(path) as f:
            v = f.read().strip()
        return None if v in ("max", "") else int(v.split()[0])
    except Exception:
        return None


def _mem() -> tuple[int, int, int]:
    # cgroup v2
    cur = _read_int("/sys/fs/cgroup/memory.current")
    peak = _read_int("/sys/fs/cgroup/memory.peak")
    limit = _read_int("/sys/fs/cgroup/memory.max")
    if cur is None:  # cgroup v1
        cur = _read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        peak = _read_int("/sys/fs/cgroup/memory/memory.max_usage_in_bytes")
        limit = _read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if cur is None:  # fallback: process RSS (no cgroup, e.g. local macOS/BSD)
        try:
            import resource
            import sys
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # ru_maxrss is bytes on Darwin/BSD, kilobytes on Linux
            cur = rss if sys.platform == "darwin" else rss * 1024
            peak = cur
        except Exception:
            cur = peak = 0
    return cur or 0, peak or 0, limit or 0


def _cpu_usec() -> int:
    v = _read_int("/sys/fs/cgroup/cpu.stat")  # first field "usage_usec N" — best effort
    if v is not None:
        try:
            with open("/sys/fs/cgroup/cpu.stat") as f:
                for line in f:
                    if line.startswith("usage_usec"):
                        return int(line.split()[1])
        except Exception:
            pass
    v = _read_int("/sys/fs/cgroup/cpuacct/cpuacct.usage")
    if v is not None:
        return v // 1000
    try:
        import time as _t
        return int(_t.process_time() * 1e6)
    except Exception:
        return 0
