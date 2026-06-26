"""The worker poll loop — one Batch job = one of these = many tasks.

A worker repeatedly claims a task, runs the stage's compute, writes the output +
completion marker, and acks. It heartbeats its lease while busy (so long tasks are
not redelivered) and exits after an idle stretch (so the fleet drains to zero
between stages). This is the entire per-job boilerplate the library absorbs: env
detection, marker skip, telemetry, S3 I/O, and the run/ack/retry dance.
"""

from __future__ import annotations

import os
import threading
import time
import traceback
from typing import Optional

from . import markers
from .context import Ctx
from .monitor import Monitor
from .task import Task, TaskState


def run_task(task: Task, *, queue, store, pipe, tracker=None, worker_id: str = "w0",
             lease: float = 300.0, heartbeat: float = 30.0, force: bool = False) -> bool:
    """Run one task end to end. Returns True on success/skip, False on failure."""
    # idempotent skip
    if not force and markers.has_result(store, task.key):
        if tracker:
            tracker.set_state(task.stage, task.key, TaskState.SKIPPED)
        queue.ack(task)
        return True

    stage = pipe.stages[task.stage]
    if tracker:
        tracker.set_state(task.stage, task.key, TaskState.RUNNING, {"worker": worker_id})
    markers.write_running(store, task, worker_id)

    # keep the lease alive while we work
    stop_hb = threading.Event()

    def _hb():
        while not stop_hb.wait(heartbeat):
            try:
                queue.extend(task, lease)
            except Exception:
                pass

    hb_thread = threading.Thread(target=_hb, daemon=True)
    hb_thread.start()

    mon = Monitor().start()
    try:
        ctx = Ctx(pipe, stage, task, store)
        stage.fn(ctx)
        outputs = ctx.finalize()
        info = mon.stop()
        info["outputs"] = outputs
        info["worker"] = worker_id
        info["ts"] = time.time()
        markers.write_result(store, task, info)
        markers.clear_running(store, task.key)
        if tracker:
            tracker.set_state(task.stage, task.key, TaskState.SUCCESS,
                              {"peak_mem_mb": info.get("peak_mem_mb"),
                               "cpu_seconds": info.get("cpu_seconds")})
        queue.ack(task)
        print(f"DONE {task.key} peak_mem={info.get('peak_mem_mb')}MB "
              f"cpu={info.get('cpu_seconds')}s", flush=True)
        return True
    except Exception:
        mon.stop()
        err = traceback.format_exc()
        print(f"FAIL {task.key}\n{err}", flush=True)
        markers.clear_running(store, task.key)
        if tracker:
            tracker.set_state(task.stage, task.key, TaskState.FAILED,
                              {"error": err.splitlines()[-1][:300]})
        queue.fail(task)
        return False
    finally:
        stop_hb.set()


def poll_loop(*, queue, store, pipe, tracker=None, worker_id: str = "w0",
              idle_timeout: float = 60.0, lease: float = 300.0, heartbeat: float = 30.0,
              force: bool = False, stop: Optional[threading.Event] = None) -> None:
    """Claim/run/ack until the queue is idle for ``idle_timeout`` seconds."""
    idle_since: Optional[float] = None
    while stop is None or not stop.is_set():
        task = queue.claim(lease)
        if task is None:
            now = time.time()
            idle_since = idle_since or now
            if now - idle_since >= idle_timeout:
                return
            time.sleep(0.2)
            continue
        idle_since = None
        run_task(task, queue=queue, store=store, pipe=pipe, tracker=tracker,
                 worker_id=worker_id, lease=lease, heartbeat=heartbeat, force=force)


# ---------------------------------------------------------------------------
# Container entrypoint: `python -m gleanflow.worker`
# ---------------------------------------------------------------------------
def main() -> None:
    import importlib

    from .config import PipelineConfig
    from .queue.sqs import SqsQueue
    from .store import store_from_config

    spec = os.environ["GLEANFLOW_PIPELINE"]          # "module.path:attr"
    mod_name, _, attr = spec.partition(":")
    pipe = getattr(importlib.import_module(mod_name), attr or "pipe")

    cfg: PipelineConfig = pipe.config
    cfg.region = os.environ.get("AWS_REGION", cfg.region)
    cfg.s3_root = os.environ.get("OUT_S3", cfg.s3_root)
    cfg.sqs_url = os.environ.get("SQS_URL", cfg.sqs_url)

    store = store_from_config(cfg)
    queue = SqsQueue(cfg.sqs_url, region=cfg.region)
    worker_id = os.environ.get("AWS_BATCH_JOB_ID", f"local-{os.getpid()}")
    poll_loop(queue=queue, store=store, pipe=pipe, worker_id=worker_id,
              idle_timeout=cfg.worker_idle_timeout, lease=cfg.lease_seconds,
              heartbeat=cfg.heartbeat_seconds, force=bool(os.environ.get("FORCE")))


if __name__ == "__main__":
    main()
