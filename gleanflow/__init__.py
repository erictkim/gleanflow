"""gleanflow — declarative chunked data pipelines on a scalable Batch worker fleet.

Author a DAG of stages as plain functions; the library decouples that high-level
compute description from the implementation (chunking, S3 staging, a leased task
queue, a self-scaling worker fleet, OOM telemetry, resumable runs). Run it locally
with zero AWS, then flip ``backend="aws"`` unchanged.

    from gleanflow import Pipeline, PipelineConfig

    pipe = Pipeline("demo", PipelineConfig(s3_root="./out"))
    src  = pipe.source("days", chunks=[{"id": d, "params": {"date": d}} for d in days])

    @pipe.stage(reads=src, target_rows=2)
    def build(ctx):
        ctx.write([{"date": m["date"], "n": 1} for m in ctx.input()])

    pipe.run(backend="local", viz=True)
"""

from .config import PipelineConfig
from .context import Ctx
from .partition import Chunk
from .pipeline import Pipeline, Source
from .stage import Stage
from .task import Task, TaskState

__all__ = [
    "Pipeline", "PipelineConfig", "Source", "Stage", "Ctx",
    "Chunk", "Task", "TaskState",
]

__version__ = "0.1.0"
