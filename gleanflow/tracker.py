"""Live run state for the visualization layer.

A thread-safe snapshot of the whole run: every stage (a group), its dependency
edges, and every task's state (queued / running / success / failed / ...). The
controller and the local workers push transitions here; the web server reads
``snapshot()`` as JSON. On the AWS backend the controller reconstructs the same
shape by polling markers + queue depth, so the dashboard works identically.
"""

from __future__ import annotations

import threading
import time

from .task import TaskState


class Tracker:
    def __init__(self, run_id: str = "run"):
        self.run_id = run_id
        self._lock = threading.Lock()
        self._stages: dict[str, dict] = {}   # name -> {deps, kind, tasks:{key:info}}
        self._order: list[str] = []
        self._events: list[dict] = []
        self._subs: list = []                # push subscribers: fn(event_dict)
        self.started_at = time.time()

    # ---- pub/sub: push status updates instead of polling -------------------
    def subscribe(self, fn) -> "callable":
        """Register fn(event) to be called on every status change. Returns an
        unsubscribe callable. Subscribers run outside the lock; exceptions are
        swallowed so a bad subscriber can never break the pipeline."""
        with self._lock:
            self._subs.append(fn)
        return lambda: self._subs.remove(fn) if fn in self._subs else None

    def emit(self, event: dict) -> None:
        for fn in list(self._subs):
            try:
                fn(event)
            except Exception:
                pass

    def add_stage(self, name: str, deps: list[str], kind: str = "stage") -> None:
        with self._lock:
            if name not in self._stages:
                self._stages[name] = {"deps": list(deps), "kind": kind, "tasks": {}}
                self._order.append(name)

    def set_tasks(self, stage: str, keys: list[str]) -> None:
        with self._lock:
            tasks = self._stages[stage]["tasks"]
            for k in keys:
                tasks.setdefault(k, {"state": TaskState.PENDING.value, "ts": time.time()})

    def set_state(self, stage: str, key: str, state, info: dict | None = None) -> None:
        s = state.value if isinstance(state, TaskState) else str(state)
        now = time.time()
        with self._lock:
            st = self._stages.setdefault(stage, {"deps": [], "kind": "stage", "tasks": {}})
            t = st["tasks"].setdefault(key, {})
            t["state"] = s
            t["ts"] = now
            if info:
                t.update(info)
            self._events.append({"ts": now, "stage": stage, "key": key, "state": s})
            if len(self._events) > 5000:
                self._events = self._events[-2000:]
        event = {"type": "task", "stage": stage, "key": key, "state": s, "ts": now}
        if info:
            event.update({k: info[k] for k in ("peak_mem_mb", "error") if k in info})
        self.emit(event)

    def counts(self, stage: str) -> dict:
        with self._lock:
            c: dict[str, int] = {}
            for t in self._stages.get(stage, {}).get("tasks", {}).values():
                c[t["state"]] = c.get(t["state"], 0) + 1
            return c

    def snapshot(self) -> dict:
        with self._lock:
            stages = []
            for name in self._order:
                st = self._stages[name]
                tasks = [{"key": k, **v} for k, v in st["tasks"].items()]
                tasks.sort(key=lambda x: x["key"])
                counts: dict[str, int] = {}
                for t in tasks:
                    counts[t["state"]] = counts.get(t["state"], 0) + 1
                stages.append({
                    "name": name, "deps": st["deps"], "kind": st["kind"],
                    "counts": counts, "tasks": tasks,
                })
            return {
                "run_id": self.run_id,
                "started_at": self.started_at,
                "now": time.time(),
                "stages": stages,
            }
