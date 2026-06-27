# gleanflow

Declarative chunked data pipelines on a scalable AWS Batch worker fleet.

You write a DAG of stages as plain Python functions. gleanflow decouples that
**high-level compute description** from the **implementation** — chunking, S3
staging, a leased task queue, a self-scaling worker fleet, OOM telemetry, and
resumable runs. Run it locally with zero AWS, then flip one flag to run it on Batch.

```python
from gleanflow import Pipeline, PipelineConfig

pipe = Pipeline("zz", PipelineConfig(s3_root="s3://my-bucket/zz", max_workers=64))
days = pipe.source("days", chunks=[{"id": f"date={d}", "params": {"date": d}} for d in dates])

@pipe.stage(reads=days, target_rows=1_000_000)        # auto-chunked into tasks
def build_rows(ctx):
    ctx.write(build(ctx.input()))                     # your compute; S3 I/O is automatic

@pipe.stage(reads=build_rows, vcpu=8, mem=32768, checkpoint=True)
def train(ctx):
    clf = ctx.checkpoint.fit(make_xgb(), *features(ctx.input()), rounds=500, every=50)
    ctx.write_model(clf)

pipe.run(backend="local", viz=True)                   # dev: in-process, no AWS
# gleanflow run examples.zz.pipeline:pipe --backend aws --workers 64
```

## The model: three planes

1. **Task plane** — the DAG expands into chunks; each chunk is a `Task` on a durable,
   **leased** work queue (`LocalQueue` / SQS). Tasks are data, not pinned to a machine.
2. **Compute plane** — a **worker fleet** (`LocalFleet` threads / Batch jobs). One job
   runs a poll loop that drains **many** tasks. The fleet scales on queue depth,
   **independent of task count**, and idle workers self-terminate. This is the
   Snowflake "warehouse": compute sized separately from the workload.
3. **Control plane** — the `Controller` topo-sorts the DAG, enqueues each stage,
   **skips already-done chunks** (markers ⇒ idempotent resume), runs the **heaviest
   chunk as a smoke test** before fan-out, and keeps the fleet sized to demand.

The `(queue, fleet)` pair is the seam that separates description from implementation:
the same pipeline runs with `backend="local"` (no AWS) or `backend="aws"` (SQS + Batch)
with **no change to stage code**.

## Why it solves uneven-size OOMs

- **Target-size chunking** caps each task's working set (`target_rows` / custom
  `partition` hook); a skewed key can't blow up one job.
- **Heaviest-first smoke gate** runs the worst chunk first and aborts fan-out if it
  OOMs — you fail one job, not a thousand.
- **Per-stage resources** (`vcpu`/`mem`, Fargate-granularity-validated) + streamed
  `PartWriter` (row-capped parts) keep memory bounded.
- **Lease redelivery + DLQ + retries** absorb Spot evictions; `ctx.checkpoint` resumes
  a fit instead of recomputing.
- A cgroup **monitor** records peak memory into each completion marker (OOM margin).

## Polars map→reduce

`gleanflow.polars` splits a lazy polars query over many parquet files into small tasks:
each **map** task scans its partition and writes a partial; one **reduce** task combines
them. `groupby_agg` auto-derives the reduce for combinable aggregations (sum/min/max/
count/mean); `map_reduce` takes explicit `map`/`reduce` callables.

```python
from gleanflow.polars import parquet_source, groupby_agg
src = parquet_source(pipe, "events", files)
groupby_agg(pipe, "by_user", source=src, by="user",
            aggs=[("hits", "count", None), ("spend", "sum", "amount"), ("avg", "mean", "amount")],
            target_bytes=512_000_000)   # pack ~512MB of files per map task
```

## LLM failure agent

When a chunk dead-letters, gleanflow can hand the failure to an agent that diagnoses
and (bounded) fixes it. The pipeline already persists everything the agent needs:
`failures/<key>.json` (traceback + peak-mem vs limit + resources), healthy-peer stats,
and the stage source.

```python
PipelineConfig(
    failure_policy="remediate",   # "off" | "report" | "remediate"
    max_remediations=2,           # cap on auto-fixes per stage
    # failure_handler=...         # default: LLMFailureAgent (claude CLI + heuristic)
)
```

The agent returns a `Remediation` the controller applies: **resplit** an oversized
packed chunk into per-partition tasks (updates the manifest so downstream follows),
**retry_with** more memory, **skip**, or **abort** with a written diagnosis. The LLM is
pluggable — pass any `complete(prompt) -> str`; the default shells out to the `claude`
CLI (your existing auth), and a deterministic OOM heuristic runs if no LLM is reachable.

The viz server also exposes a **local query API** the agent (or you) can curl:
`/api/state`, `/api/failures`, `/api/task?key=<stage/chunk>`, `/api/stage?name=<stage>`.

**The webserver can also call Claude back.** With `enable_agent_api=True`, POST
endpoints spawn a `claude -p` session on demand against the run's context:
`POST /api/diagnose?key=<stage/chunk>` (root-cause + suggested fix),
`POST /api/ask {question, key}`, `POST /api/check` (whole-run health verdict). The
dashboard wires these up — a **Check run ▶** button and click-a-failed-square-to-diagnose.
Local-only and opt-in, since it spawns a subprocess from an HTTP request.

## Visualization

`pipe.run(viz=True)` (or `--viz`) starts a local dashboard at
`http://127.0.0.1:8765`: each **stage is a group**, each **task is a small square**
colored by state (queued · running · success · failed · skipped), with SVG edges
showing how one group of tasks feeds the next. Live, dependency-free, works on both
backends. Serve a saved snapshot with `gleanflow viz snapshot.json`.

## CLI

```
gleanflow run    pkg.mod:pipe [--backend local|aws] [--workers N] [--viz] [--force] k=v...
gleanflow status pkg.mod:pipe
gleanflow infra  apply|destroy pkg.mod:pipe     # always-provision Terraform
gleanflow worker pkg.mod:pipe                   # one local poll-loop worker
gleanflow viz    snapshot.json [--port P]
```

## AWS (always-provision)

`gleanflow infra apply pkg.mod:pipe` renders and applies Terraform that owns: a
Fargate-Spot Batch compute environment, one generic **worker** job definition, the
Batch queue, the **SQS task queue + DLQ**, an S3 bucket, ECR, and IAM. Build/push the
worker image (`gleanflow/infra/Dockerfile`), set `config.image`, then
`gleanflow run ... --backend aws`. User code is delivered to workers at runtime as a
content-hashed zip (`APPCODE_S3`) — no image rebuild for code changes.

## Layout

```
gleanflow/
  pipeline.py stage.py context.py     # authoring API (the description)
  partition.py store.py markers.py    # chunking, object store, completion markers
  task.py queue/                      # task plane (leased queue: local + sqs)
  worker.py fleet/                    # compute plane (poll loop + local/batch fleet)
  controller.py tracker.py            # control plane + live state for viz
  monitor.py ckpt.py                  # OOM telemetry, resumable fit
  web/                                # local dashboard (stdlib http.server + vanilla JS)
  infra/                              # Terraform + Dockerfile + entrypoint
examples/zz/pipeline.py               # runnable fan-out/fan-in demo
tests/                                # local-backend e2e + queue/packer units
```

## Test

```
pip install -e .[dev]
pytest
```

Covers: end-to-end local run, `workers=1` finishing all tasks (one worker, many
tasks), worker-count-independent results, marker-based resume/skip, smoke-gate abort,
and queue lease/redelivery/DLQ.
