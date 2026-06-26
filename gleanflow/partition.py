"""Chunking: turn a stage's upstream into a balanced set of work chunks.

Hybrid model:
  * **auto size-target packer** (``pack``) — greedily groups members into chunks
    whose summed ``weight`` is ~``target``, capping each task's working set. This
    is the OOM fix: no single task gets an unbounded slice.
  * **custom hook** — a stage may instead supply ``partition=fn(upstream, cfg)``
    returning its own ``Chunk`` list for skewed keys.

Chunks are ordered **heaviest-first** so the controller's smoke test exercises the
worst case before fanning out.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class Chunk:
    id: str                          # stable id within a stage (used to build task keys)
    params: dict = field(default_factory=dict)
    weight: float = 1.0

    def to_dict(self) -> dict:
        return {"id": self.id, "params": self.params, "weight": self.weight}

    @classmethod
    def from_dict(cls, d: dict) -> "Chunk":
        return cls(id=d["id"], params=d.get("params", {}), weight=d.get("weight", 1.0))


def heaviest_first(chunks: Iterable[Chunk]) -> list[Chunk]:
    return sorted(chunks, key=lambda c: -c.weight)


def _ref(m: Chunk) -> dict:
    """A reference to an upstream chunk: its id (to locate its data) + params."""
    return {"id": m.id, "params": m.params}


def pack(members: list[Chunk], target: float, prefix: str = "c") -> list[Chunk]:
    """Greedily pack ``members`` into chunks of ~``target`` total weight.

    Each output chunk carries ``params={"_members": [{id, params}...]}`` so the
    stage can locate and iterate its slice. A single member heavier than ``target``
    becomes its own chunk (we never split a member — the producer that defined the
    member is responsible for it being individually tractable).
    """
    if target <= 0:
        return one_to_one(members, prefix)

    out: list[Chunk] = []
    cur: list[Chunk] = []
    cur_w = 0.0

    def flush():
        nonlocal cur, cur_w
        if cur:
            out.append(Chunk(
                id=f"{prefix}{len(out)}",
                params={"_members": [_ref(m) for m in cur]},
                weight=cur_w,
            ))
            cur, cur_w = [], 0.0

    for m in members:
        if cur and cur_w + m.weight > target:
            flush()
        cur.append(m)
        cur_w += m.weight
    flush()
    return out


def one_to_one(members: list[Chunk], prefix: str = "c") -> list[Chunk]:
    """Map each upstream chunk to exactly one task chunk (no repacking).

    Preserves the upstream id (handy for debugging + viz labels) and records the
    single member reference under ``_members``.
    """
    return [Chunk(id=m.id, params={"_members": [_ref(m)]}, weight=m.weight)
            for m in members]
