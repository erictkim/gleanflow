"""The unit that flows on the task plane.

A ``Task`` is pure data — a chunk of work for one stage. It is *not* tied to any
machine; it lives on the work queue and any worker may claim it. This decoupling
is what lets one Batch job run many tasks and lets the fleet scale independently
of how many tasks exist.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"    # known, deps not yet satisfied / not enqueued
    QUEUED = "queued"      # on the work queue, awaiting a worker
    RUNNING = "running"    # claimed by a worker, executing
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"    # marker already existed -> no recompute


@dataclass
class Task:
    stage: str                       # stage name this task belongs to
    key: str                         # globally-unique key; also the marker path
    params: dict = field(default_factory=dict)
    weight: float = 1.0              # heaviness estimate -> heaviest-first smoke gate
    attempt: int = 0                 # incremented on each (re)delivery

    def to_json(self) -> str:
        return json.dumps(
            {"stage": self.stage, "key": self.key, "params": self.params,
             "weight": self.weight, "attempt": self.attempt},
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, s: str) -> "Task":
        d = json.loads(s)
        return cls(stage=d["stage"], key=d["key"], params=d.get("params", {}),
                   weight=d.get("weight", 1.0), attempt=d.get("attempt", 0))
