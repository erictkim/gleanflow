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

        # LLM failure agent (opt-in via cfg.failure_policy)
        self.handler = None
        if self.cfg.failure_policy in ("report", "remediate"):
            from .agent import default_agent
            self.handler = self.cfg.failure_handler or default_agent()

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
            investigator = None
            if self.cfg.enable_agent_api:
                from .agent import Investigator
                investigator = Investigator(self.store, self.pipe)
            self._viz_server = start_server(self.tracker, store=self.store, pipe=self.pipe,
                                            investigator=investigator,
                                            host=self.cfg.viz_host, port=self.cfg.viz_port)
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
            self._drain([smoke_t], stage, queue, fleet, gate=True)
            rest = pending[1:]

        for t in rest:
            queue.enqueue(t)
            self.tracker.set_state(stage.name, t.key, TaskState.QUEUED)
        fleet.ensure(min(self.max_workers, max(1, queue.depth())))
        self._drain(rest, stage, queue, fleet, gate=False)

    def _drain(self, tasks, stage, queue, fleet, *, gate: bool):
        """Wait for ``tasks`` to finish; on a dead-lettered chunk, invoke the agent
        and (under budget) apply its remediation, growing/shrinking the active set."""
        active = {t.key: t for t in tasks}
        budget = self.cfg.max_remediations
        deadline = time.time() + 6 * 3600
        while time.time() < deadline:
            depth = queue.depth()
            if depth:
                fleet.ensure(min(self.max_workers, depth + fleet.running()))
            if self.backend != "local":
                self._refresh_from_markers(stage, list(active.values()))

            dlq = getattr(queue, "dlq", [])
            dead = [active[t.key] for t in list(dlq) if t.key in active]
            if dead:
                new_tasks = self._handle_failures(stage, dead, queue, fleet, budget)
                if new_tasks is None:
                    msg = (f"{stage.name}: {len(dead)} chunk(s) failed"
                           + ("" if self.handler else " (no failure agent configured)"))
                    raise SmokeFailed(msg) if gate else StageFailed(msg)
                budget -= 1
                for d in dead:
                    active.pop(d.key, None)
                    try:
                        dlq.remove(d)
                    except ValueError:
                        pass
                for nt in new_tasks:
                    active[nt.key] = nt
                continue

            if all(markers.has_result(self.store, k) for k in active):
                return
            time.sleep(0.05 if self.backend == "local" else 5.0)
        raise StageFailed(f"{stage.name}: timed out waiting for {len(active)} chunks")

    # ---- failure agent ----------------------------------------------------
    def _handle_failures(self, stage, dead, queue, fleet, budget):
        """Return new tasks to keep waiting on, or None to fail the stage."""
        if self.handler is None:
            return None
        from .agent import DiagnosticTools, Remediation
        tools = DiagnosticTools(self.store, self.pipe)
        new: list = []
        for t in dead:
            self.tracker.set_state(stage.name, t.key, TaskState.FAILED,
                                   {"error": "dead-lettered"})
            event = self._failure_event(stage, t, tools)
            rem = self.handler(event, tools) or Remediation("report")
            print(f"[agent] {t.key}: action={rem.action} :: {rem.diagnosis} "
                  f"-> {rem.fix}", flush=True)
            if (self.cfg.failure_policy != "remediate" or budget <= 0
                    or rem.action in ("report", "abort")):
                return None
            if rem.action == "retry_with":
                new.append(self._retry_with(stage, t, rem, queue, fleet))
            elif rem.action == "resplit":
                new += self._resplit(stage, t, rem, queue, fleet)
            elif rem.action == "skip":
                self._skip(stage, t)
            else:
                return None
        return new

    def _failure_event(self, stage, task, tools):
        from .agent import FailureEvent
        f = {}
        if markers.has_failure(self.store, task.key):
            try:
                f = markers.read_failure(self.store, task.key)
            except Exception:
                f = {}
        peers = tools.peer_stats(stage.name)
        return FailureEvent(
            stage=stage.name, task_key=task.key, params=task.params, attempt=task.attempt,
            error=f.get("error", ""), traceback=f.get("traceback", ""),
            peak_mem_mb=f.get("peak_mem_mb"), limit_mb=f.get("limit_mb"),
            cpu_seconds=f.get("cpu_seconds"),
            members=task.params.get("_in", {}).get("members", []),
            resources=f.get("resources") or stage.resources(self.cfg),
            peer_peak_mem_mb=peers.get("max_peak_mem_mb"),
        )

    def _retry_with(self, stage, task, rem, queue, fleet):
        # bump the fleet's worker resources (AWS); local backend has no per-task mem
        if hasattr(fleet, "resources"):
            if rem.mem:
                fleet.resources["mem"] = rem.mem
            if rem.vcpu:
                fleet.resources["vcpu"] = rem.vcpu
        task.attempt = 0
        queue.enqueue(task)
        self.tracker.set_state(stage.name, task.key, TaskState.QUEUED, {"retry": True})
        return task

    def _resplit(self, stage, parent, rem, queue, fleet):
        members = parent.params.get("_in", {})
        mlist = members.get("members", [])
        if len(mlist) <= 1:
            # nothing to split — fall back to a memory bump
            from .agent import Remediation
            return [self._retry_with(stage, parent, Remediation(
                "retry_with", mem=((stage.resources(self.cfg)["mem"] * 2 + 4095) // 4096) * 4096),
                queue, fleet)]
        pid = parent.params["_chunk_id"]
        subs = []
        for j, m in enumerate(mlist):
            sid = f"{pid}-m{j}"
            p = {"_chunk_id": sid,
                 "_in": {"kind": members["kind"], "name": members["name"], "members": [m]}}
            subs.append(Task(stage=stage.name, key=f"{stage.name}/{sid}",
                             params=p, weight=parent.weight / len(mlist)))
        self._manifest_replace(stage.name, pid,
                               [{"id": s.params["_chunk_id"], "params": {}, "weight": s.weight}
                                for s in subs])
        self.tracker.set_tasks(stage.name, [s.key for s in subs])
        for s in subs:
            queue.enqueue(s)
            self.tracker.set_state(stage.name, s.key, TaskState.QUEUED,
                                   {"resplit_of": parent.key})
        return subs

    def _skip(self, stage, task):
        self.tracker.set_state(stage.name, task.key, TaskState.SKIPPED,
                               {"skipped_by": "agent"})
        self._manifest_replace(stage.name, task.params["_chunk_id"], [])

    def _manifest_replace(self, stage_name, old_id, new_entries):
        key = f"data/{stage_name}/_manifest.json"
        m = json.loads(self.store.get_bytes(key).decode())
        m["chunks"] = [c for c in m["chunks"] if c["id"] != old_id] + new_entries
        self.store.put_bytes(key, json.dumps(m).encode())

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
