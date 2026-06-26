"""The control plane: expand the DAG into tasks and drive a backend.

Per stage, in topological order, the controller:
  1. enumerates chunks (auto packer / custom hook) from the upstream manifest,
     writes this stage's manifest, builds tasks;
  2. **skips chunks whose completion marker already exists** (idempotent resume);
  3. runs the **heaviest chunk as a smoke test** and gates fan-out on it;
  4. enqueues the rest and keeps the **fleet sized to queue depth** (never to task
     count) until the stage drains (a barrier — downstream needs this manifest).

It also owns the ``Tracker`` the viz layer reads, updating it directly on the local
backend and from markers on the AWS backend.
"""

from __future__ import annotations

import json
import time

from . import markers
from .partition import Chunk, heaviest_first, one_to_one, pack
from .store import store_from_config
from .task import Task, TaskState
from .tracker import Tracker


class SmokeFailed(RuntimeError):
    pass


class StageFailed(RuntimeError):
    pass


class Controller:
    def __init__(self, pipe, *, backend="local", workers=None, viz=False,
                 force=False, smoke=True):
        self.pipe = pipe
        self.cfg = pipe.config
        self.backend = backend
        self.max_workers = workers or self.cfg.max_workers
        self.viz = viz
        self.force = force
        self.smoke = smoke
        self.store = store_from_config(self.cfg)
        self.tracker = Tracker(run_id=f"{pipe.name}")
        self._viz_server = None

    # ---- backend wiring ---------------------------------------------------
    def _make_queue_fleet(self):
        if self.backend == "local":
            from .fleet.local import LocalFleet
            from .queue.local import LocalQueue
            q = LocalQueue(max_redeliveries=self.cfg.max_redeliveries)
            f = LocalFleet(queue=q, store=self.store, pipe=self.pipe, tracker=self.tracker,
                           idle_timeout=2.0, lease=self.cfg.lease_seconds,
                           heartbeat=self.cfg.heartbeat_seconds, force=self.force)
            return q, f
        if self.backend == "aws":
            from .fleet.batch import BatchFleet
            from .queue.sqs import SqsQueue
            spec = self.cfg.extra.get("pipeline_spec", "")
            q = SqsQueue(self.cfg.sqs_url, region=self.cfg.region)
            f = BatchFleet(self.cfg, pipeline_spec=spec)
            if self.cfg.extra.get("package_dir"):
                f.deliver_code(self.cfg.extra["package_dir"])
            return q, f
        raise ValueError(f"unknown backend {self.backend!r}")

    # ---- chunk planning ---------------------------------------------------
    def _upstream_chunks(self, stage, run_args) -> list[Chunk]:
        up = stage.reads
        if up is None:
            return [Chunk(id="all", params={}, weight=1.0)]
        if up.kind == "source":
            return up.enumerate(self.cfg, **run_args)
        # stage upstream: read its manifest
        key = f"data/{up.name}/_manifest.json"
        chunks = json.loads(self.store.get_bytes(key).decode())["chunks"]
        return [Chunk.from_dict(c) for c in chunks]

    def _plan(self, stage, run_args) -> list[Task]:
        up = stage.reads
        upstream_chunks = self._upstream_chunks(stage, run_args)
        if stage.partition is not None:
            chunks = list(stage.partition(upstream_chunks, self.cfg))
        elif stage.target_rows or stage.target_bytes:
            target = stage.target_rows or stage.target_bytes
            chunks = pack(upstream_chunks, float(target))
        else:
            chunks = one_to_one(upstream_chunks)

        # persist this stage's output manifest for downstream enumeration
        manifest = {"chunks": [{"id": c.id,
                                "params": {k: v for k, v in c.params.items()
                                           if not k.startswith("_")},
                                "weight": c.weight} for c in chunks]}
        self.store.put_bytes(f"data/{stage.name}/_manifest.json",
                             json.dumps(manifest).encode())

        tasks: list[Task] = []
        for c in chunks:
            members = c.params.get("_members", [])
            params = {k: v for k, v in c.params.items() if k != "_members"}
            params["_chunk_id"] = c.id
            if up is not None:
                params["_in"] = {"kind": up.kind, "name": up.name, "members": members}
            tasks.append(Task(stage=stage.name, key=f"{stage.name}/{c.id}",
                              params=params, weight=c.weight))
        return tasks

    # ---- run --------------------------------------------------------------
    def run(self, **run_args):
        if self.viz:
            from .web.server import start_server
            self._viz_server = start_server(self.tracker, host=self.cfg.viz_host,
                                            port=self.cfg.viz_port)
            print(f"[viz] http://{self.cfg.viz_host}:{self.cfg.viz_port}", flush=True)

        # register the DAG (groups + edges) for the dashboard
        order = self.pipe.topo_order()
        for st in order:
            self.tracker.add_stage(st.name, self.pipe.deps_of(st))

        queue, fleet = self._make_queue_fleet()
        try:
            for st in order:
                self._run_stage(st, queue, fleet, run_args)
        finally:
            if self.backend == "local":
                fleet.drain()
        print(f"[done] pipeline {self.pipe.name}", flush=True)
        if self._viz_server is not None and run_args.get("hold_viz", True):
            self._hold_viz()
        return self.tracker.snapshot()

    def _run_stage(self, stage, queue, fleet, run_args):
        tasks = self._plan(stage, run_args)
        self.tracker.set_tasks(stage.name, [t.key for t in tasks])

        pending = []
        for t in tasks:
            if not self.force and markers.has_result(self.store, t.key):
                self.tracker.set_state(stage.name, t.key, TaskState.SKIPPED)
            else:
                pending.append(t)
        if not pending:
            print(f"[stage] {stage.name}: all {len(tasks)} chunks cached", flush=True)
            return

        pending = heaviest_first(pending)
        print(f"[stage] {stage.name}: {len(pending)}/{len(tasks)} chunks to run", flush=True)

        rest = pending
        if self.smoke and len(pending) > 1:
            smoke_t = pending[0]
            queue.enqueue(smoke_t)
            self.tracker.set_state(stage.name, smoke_t.key, TaskState.QUEUED)
            fleet.ensure(1)
            self._wait([smoke_t], stage, queue, fleet, gate=True)
            rest = pending[1:]

        for t in rest:
            queue.enqueue(t)
            self.tracker.set_state(stage.name, t.key, TaskState.QUEUED)
        fleet.ensure(min(self.max_workers, max(1, queue.depth())))
        self._wait(rest, stage, queue, fleet, gate=False)

    def _wait(self, tasks, stage, queue, fleet, *, gate: bool):
        keys = {t.key for t in tasks}
        deadline = time.time() + 6 * 3600
        while time.time() < deadline:
            # keep the fleet topped up to demand (independent of task count)
            depth = queue.depth()
            if depth:
                fleet.ensure(min(self.max_workers, depth + fleet.running()))

            if self.backend != "local":
                self._refresh_from_markers(stage, tasks)

            done = sum(markers.has_result(self.store, k) for k in keys)
            dead = [t for t in tasks if t in getattr(queue, "dlq", [])]
            if dead:
                for t in dead:
                    self.tracker.set_state(stage.name, t.key, TaskState.FAILED,
                                           {"error": "max redeliveries -> DLQ"})
                msg = f"{stage.name}: {len(dead)} chunk(s) dead-lettered"
                raise SmokeFailed(msg) if gate else StageFailed(msg)
            if done == len(keys):
                return
            time.sleep(0.1 if self.backend == "local" else 5.0)
        raise StageFailed(f"{stage.name}: timed out waiting for {len(keys)} chunks")

    def _refresh_from_markers(self, stage, tasks):
        for t in tasks:
            if markers.has_result(self.store, t.key):
                try:
                    info = markers.read_result(self.store, t.key)
                except Exception:
                    info = {}
                self.tracker.set_state(stage.name, t.key, TaskState.SUCCESS,
                                       {"peak_mem_mb": info.get("peak_mem_mb"),
                                        "cpu_seconds": info.get("cpu_seconds")})
            elif self.store.exists(markers.running_key(t.key)):
                self.tracker.set_state(stage.name, t.key, TaskState.RUNNING)

    def _hold_viz(self):
        print("[viz] holding dashboard open — Ctrl-C to exit", flush=True)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
