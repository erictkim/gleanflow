"""Stage spec — the captured high-level compute description.

The ``@pipe.stage`` decorator turns a plain ``def f(ctx): ...`` into one of these.
Everything here is declarative metadata (deps, chunking knobs, resource profile);
the only imperative part is ``fn``, the user's compute. The worker resolves ``fn``
by name at runtime; the controller reads the metadata to plan chunks and size jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class Stage:
    name: str
    fn: Callable                      # user compute: fn(ctx) -> None
    reads: Optional[object] = None    # upstream Stage | Source (has .name, .kind)
    chunk_by: Optional[str] = None
    target_rows: Optional[int] = None
    target_bytes: Optional[int] = None
    partition: Optional[Callable] = None   # custom hook: fn(upstream_chunks, cfg) -> [Chunk]
    vcpu: Optional[int] = None
    mem: Optional[int] = None         # MB
    omp: Optional[int] = None
    retries: int = 2
    checkpoint: bool = False
    cap_rows: int = 1_000_000

    kind: str = "stage"

    @property
    def upstream_name(self) -> Optional[str]:
        return getattr(self.reads, "name", None)

    @property
    def upstream_kind(self) -> Optional[str]:
        return getattr(self.reads, "kind", None)

    def resources(self, cfg) -> dict:
        """Resolve effective vCPU/MEM/OMP for this stage, with Fargate validation."""
        vcpu = self.vcpu or cfg.default_vcpu
        mem = self.mem or cfg.default_mem
        # Fargate memory granularity: must be a 4096-MB multiple at >=8 vCPU.
        if vcpu >= 8 and mem % 4096 != 0:
            mem = ((mem + 4095) // 4096) * 4096
        return {"vcpu": vcpu, "mem": mem, "omp": self.omp or vcpu}
