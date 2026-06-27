"""Native LLM failure agent: notice a failed chunk, diagnose it, optionally fix it.

When a chunk dead-letters, the controller assembles a ``FailureEvent`` (the failing
task, its traceback, the monitor's peak-mem vs limit, healthy-peer stats, the stage
source) and hands it to a ``FailureHandler``. ``LLMFailureAgent`` asks an LLM for a
structured ``Remediation`` — the controller then applies it, **bounded**: re-split the
oversized chunk, retry it with more memory, skip it, or abort with a written diagnosis.

The LLM is pluggable: pass any ``complete(prompt) -> str`` callable. The default shells
out to the ``claude`` CLI (uses your existing Claude Code auth — no API key plumbing).
If no LLM is reachable, a deterministic heuristic still handles the common OOM case, so
the agent degrades gracefully offline.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------
@dataclass
class FailureEvent:
    stage: str
    task_key: str
    params: dict
    attempt: int
    error: str = ""
    traceback: str = ""
    peak_mem_mb: Optional[float] = None
    limit_mb: Optional[float] = None
    cpu_seconds: Optional[float] = None
    members: list = field(default_factory=list)     # input files / upstream chunk refs
    resources: dict = field(default_factory=dict)   # stage vcpu/mem
    peer_peak_mem_mb: Optional[float] = None        # max peak among successful siblings
    log_tail: str = ""

    @property
    def n_members(self) -> int:
        return len(self.members)

    @property
    def near_oom(self) -> bool:
        return bool(self.peak_mem_mb and self.limit_mb and
                    self.peak_mem_mb >= 0.9 * self.limit_mb)


@dataclass
class Remediation:
    action: str = "report"            # retry_with | resplit | skip | abort | report
    mem: Optional[int] = None         # retry_with: new memory (MB)
    vcpu: Optional[int] = None
    factor: int = 0                   # resplit: split each member out (0 -> per-member)
    diagnosis: str = ""
    fix: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "Remediation":
        return cls(action=d.get("action", "report"), mem=d.get("mem"),
                   vcpu=d.get("vcpu"), factor=int(d.get("factor", 0)),
                   diagnosis=d.get("diagnosis", ""), fix=d.get("fix", ""))


# read-only helpers the agent may consult while diagnosing
class DiagnosticTools:
    def __init__(self, store, pipe):
        self.store = store
        self.pipe = pipe

    def read_marker(self, key: str) -> dict:
        from . import markers
        return markers.read_result(self.store, key)

    def read_failure(self, key: str) -> dict:
        from . import markers
        return markers.read_failure(self.store, key)

    def list_failures(self, stage: str) -> list[str]:
        from . import markers
        return markers.list_failures(self.store, stage)

    def peer_stats(self, stage: str) -> dict:
        from . import markers
        peaks, n = [], 0
        for k in self.store.list(f"results/{stage}/"):
            if not k.endswith(".json"):
                continue
            try:
                info = markers.read_result(self.store, k[len("results/"):-len(".json")])
            except Exception:
                continue
            n += 1
            if info.get("peak_mem_mb"):
                peaks.append(info["peak_mem_mb"])
        return {"success": n, "max_peak_mem_mb": max(peaks) if peaks else None}

    def read_stage_source(self, stage: str) -> str:
        import inspect
        try:
            return inspect.getsource(self.pipe.stages[stage].fn)
        except Exception:
            return ""


# a handler is any callable(event, tools) -> Remediation | None
FailureHandler = Callable[[FailureEvent, DiagnosticTools], Optional[Remediation]]


# ---------------------------------------------------------------------------
# LLM backends
# ---------------------------------------------------------------------------
def claude_cli_complete(prompt: str, *, timeout: float = 120.0) -> str:
    """Default LLM: the `claude` CLI in headless print mode (uses existing auth)."""
    if shutil.which("claude") is None:
        raise RuntimeError("`claude` CLI not on PATH")
    r = subprocess.run(["claude", "-p", prompt], capture_output=True, text=True,
                       timeout=timeout)
    return r.stdout


_PROMPT = """\
You are an SRE agent triaging a failed data-pipeline chunk on AWS Batch.

stage: {stage}
chunk: {key}   attempt {attempt}
members (input partitions in this chunk): {n_members}
stage resources: {resources}
peak memory: {peak} MB   container limit: {limit} MB
healthy sibling chunks peak: {peer} MB

error: {error}

traceback (tail):
{tb}

stage source:
{src}

Decide ONE remediation and reply with ONLY a JSON object, no prose:
{{"action": "retry_with|resplit|skip|abort|report",
  "mem": <new memory MB or null>, "vcpu": <new vcpu or null>,
  "factor": <0 to split this chunk's members into separate tasks, else 0>,
  "diagnosis": "<one sentence root cause>", "fix": "<one sentence>"}}

Guidance: an out-of-memory chunk that packs several members should usually be
"resplit"; a single-member chunk that still OOMs should "retry_with" ~2x mem
(Fargate memory must be a multiple of 4096). Use "abort" for a genuine code/data
bug the agent cannot fix.
"""


class LLMFailureAgent:
    """A FailureHandler backed by a pluggable LLM, with an offline heuristic fallback."""

    def __init__(self, complete: Optional[Callable[[str], str]] = None, *,
                 notify: Optional[Callable[[str], None]] = None):
        self.complete = complete or claude_cli_complete
        self.notify = notify or (lambda m: print(m, flush=True))

    def __call__(self, event: FailureEvent, tools: DiagnosticTools) -> Optional[Remediation]:
        rem = self._ask(event, tools)
        self.notify(f"[agent] {event.stage}/{event.task_key}: {rem.action} — "
                    f"{rem.diagnosis} :: {rem.fix}")
        return rem

    def _ask(self, event: FailureEvent, tools: DiagnosticTools) -> Remediation:
        prompt = _PROMPT.format(
            stage=event.stage, key=event.task_key, attempt=event.attempt,
            n_members=event.n_members, resources=event.resources,
            peak=event.peak_mem_mb, limit=event.limit_mb, peer=event.peer_peak_mem_mb,
            error=event.error, tb=(event.traceback or "")[-1500:],
            src=tools.read_stage_source(event.stage)[:2000],
        )
        try:
            reply = self.complete(prompt)
            m = re.search(r"\{.*\}", reply, re.DOTALL)
            if m:
                return Remediation.from_dict(json.loads(m.group(0)))
        except Exception as e:
            self.notify(f"[agent] LLM unavailable ({e}); using heuristic")
        return _heuristic(event)


def _heuristic(event: FailureEvent) -> Remediation:
    """Deterministic OOM triage when no LLM is reachable."""
    oom = event.near_oom or any(s in (event.error + event.traceback).lower()
                                for s in ("memoryerror", "oom", "out of memory", "killed"))
    if oom and event.n_members > 1:
        return Remediation("resplit", diagnosis="OOM on a multi-file chunk",
                           fix="split the chunk into per-partition tasks")
    if oom:
        cur = event.resources.get("mem", 16384)
        new = ((cur * 2 + 4095) // 4096) * 4096       # Fargate granularity
        return Remediation("retry_with", mem=new,
                           diagnosis="OOM on a single-partition chunk",
                           fix=f"retry with {new} MB")
    return Remediation("report", diagnosis="non-memory failure",
                       fix="needs human review")


def default_agent() -> LLMFailureAgent:
    """The bounded-auto-remediate agent: claude CLI + stdout, heuristic fallback."""
    return LLMFailureAgent()
