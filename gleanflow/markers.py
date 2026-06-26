"""Completion markers: the single source of truth for "this chunk is done".

A worker writes ``results/<task-key>.json`` after a task succeeds. The controller
polls these to (a) advance stages and (b) **skip already-done chunks on a re-run**
(idempotent resume). A lightweight ``running/<task-key>.json`` heartbeat lets the
controller (and the viz layer) see in-flight work even on the AWS backend.
"""

from __future__ import annotations

import json
import time

from .store import Store


def result_key(task_key: str) -> str:
    return f"results/{task_key}.json"


def running_key(task_key: str) -> str:
    return f"running/{task_key}.json"


def has_result(store: Store, task_key: str) -> bool:
    return store.exists(result_key(task_key))


def read_result(store: Store, task_key: str) -> dict:
    return json.loads(store.get_bytes(result_key(task_key)).decode())


def write_result(store: Store, task, info: dict) -> None:
    body = {
        "job_key": task.key,
        "stage": task.stage,
        "params": task.params,
        "ts": info.get("ts", _now()),
        "peak_mem_mb": info.get("peak_mem_mb"),
        "cpu_seconds": info.get("cpu_seconds"),
        "outputs": info.get("outputs", []),
        "worker": info.get("worker"),
        "attempt": task.attempt,
    }
    store.put_bytes(result_key(task.key), json.dumps(body).encode())


def write_running(store: Store, task, worker_id: str) -> None:
    store.put_bytes(running_key(task.key),
                    json.dumps({"job_key": task.key, "stage": task.stage,
                                "worker": worker_id, "ts": _now()}).encode())


def clear_running(store: Store, task_key: str) -> None:
    # best-effort; not all stores support delete uniformly, so overwrite as cleared
    try:
        store.put_bytes(running_key(task_key), b'{"cleared":true}')
    except Exception:
        pass


def _now() -> float:
    return time.time()
