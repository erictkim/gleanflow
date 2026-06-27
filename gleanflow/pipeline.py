"""The high-level compute description: a DAG of stages.

The user builds one ``Pipeline``, decorates plain functions with ``@pipe.stage``,
and points each stage at what it ``reads``. That is the entire authoring surface —
no queue, no fleet, no S3, no Batch. ``pipe.run(...)`` hands the DAG to the
controller, which expands it into tasks and drives whichever backend is selected.
"""

from __future__ import annotations

from typing import Callable, Optional

from .config import PipelineConfig
from .stage import Stage


class Source:
    """An external input to the DAG (raw data not produced by the pipeline).

    ``enumerate_fn(cfg, **run_args) -> [Chunk]`` defines the source's chunks (e.g.
    one per date). Optional ``reader(cfg, member_params) -> [records]`` reads a
    member's bytes; if omitted, ``ctx.input()`` yields the member params themselves
    (handy for generator-style first stages that synthesize from a key).
    """

    kind = "source"

    def __init__(self, pipe: "Pipeline", name: str, *,
                 enumerate_fn: Optional[Callable] = None,
                 chunks: Optional[list] = None,
                 reader: Optional[Callable] = None,
                 uri: str = "", key: str = "key"):
        self.name = name
        self.uri = uri
        self.key = key
        self.reader = reader
        self._enumerate_fn = enumerate_fn
        self._chunks = chunks
        pipe.sources[name] = self

    def enumerate(self, cfg, **run_args):
        from .partition import Chunk
        if self._enumerate_fn is not None:
            return list(self._enumerate_fn(cfg, **run_args))
        if self._chunks is not None:
            return [c if isinstance(c, Chunk) else Chunk(**c) for c in self._chunks]
        raise ValueError(f"Source {self.name!r} has neither enumerate_fn nor chunks")


class Pipeline:
    def __init__(self, name: str, config: Optional[PipelineConfig] = None):
        self.name = name
        self.config = config or PipelineConfig()
        self.stages: dict[str, Stage] = {}
        self.sources: dict[str, Source] = {}

    # ---- authoring API ----------------------------------------------------
    def stage(self, reads=None, *, name: Optional[str] = None, chunk_by=None,
              target_rows=None, target_bytes=None, partition=None,
              vcpu=None, mem=None, omp=None, retries=2, checkpoint=False,
              cap_rows=1_000_000):
        def deco(fn: Callable) -> Stage:
            st = Stage(
                name=name or fn.__name__, fn=fn, reads=reads, chunk_by=chunk_by,
                target_rows=target_rows, target_bytes=target_bytes, partition=partition,
                vcpu=vcpu, mem=mem, omp=omp, retries=retries, checkpoint=checkpoint,
                cap_rows=cap_rows,
            )
            self.stages[st.name] = st
            return st
        return deco

    def source(self, name: str, **kw) -> Source:
        return Source(self, name, **kw)

    # ---- DAG --------------------------------------------------------------
    def topo_order(self) -> list[Stage]:
        seen: set[str] = set()
        order: list[Stage] = []

        def visit(st: Stage):
            if st.name in seen:
                return
            up = st.upstream_name
            if up and up in self.stages:
                visit(self.stages[up])
            seen.add(st.name)
            order.append(st)

        for st in self.stages.values():
            visit(st)
        return order

    def deps_of(self, st: Stage) -> list[str]:
        up = st.upstream_name
        return [up] if (up and up in self.stages) else []

    # ---- execution --------------------------------------------------------
    def run(self, backend: str = "local", *, workers: Optional[int] = None,
            viz: bool = False, force: bool = False, smoke: bool = True,
            on_event=None, **run_args):
        """Run the pipeline. ``on_event(event)`` is an in-process push callback
        invoked on every status change (task transition, stage_done, run_done)."""
        from .controller import Controller
        return Controller(self, backend=backend, workers=workers, viz=viz,
                          force=force, smoke=smoke, on_event=on_event).run(**run_args)
