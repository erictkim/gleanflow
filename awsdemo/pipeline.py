"""Self-contained gleanflow pipeline for a real AWS Batch run.

Kept as its own top-level package so the runtime appcode zip (shipped to workers via
APPCODE_S3) imports cleanly as ``awsdemo.pipeline:pipe`` inside the container.

    gleanflow infra apply awsdemo.pipeline:pipe
    gleanflow run         awsdemo.pipeline:pipe --backend aws --workers 4
"""

from __future__ import annotations

import os

from gleanflow import Pipeline, PipelineConfig

# No secrets/account ids in source. Provide your AWS account via env (or set
# AWSDEMO_IMAGE directly to a full ECR image ref).
REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT = os.environ.get("AWS_ACCOUNT_ID", "")
IMAGE = os.environ.get(
    "AWSDEMO_IMAGE",
    f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/awsdemo-gleanflow:latest" if ACCOUNT else "",
)

SIZES = {
    "20260501": 3, "20260502": 30, "20260503": 5, "20260504": 50,
    "20260505": 8, "20260506": 80, "20260507": 12,
}

pipe = Pipeline("awsdemo", PipelineConfig(
    s3_root="./.awsdemo",          # overridden by infra outputs / OUT_S3 env
    region=REGION,
    image=IMAGE,
    default_vcpu=2,
    default_mem=16384,
    max_workers=4,
    worker_idle_timeout=60,
    lease_seconds=300,
    # to pin an already-provisioned bucket, set extra={"bucket_suffix": "<suffix>"}
))

days = pipe.source("days", chunks=[
    {"id": f"date={d}", "params": {"date": d, "rows": n}, "weight": float(n)}
    for d, n in SIZES.items()
])


@pipe.stage(reads=days)
def build_rows(ctx):
    out = []
    for m in ctx.input():
        d, n = m["date"], m["rows"]
        for j in range(n):
            out.append({"date": d, "i": j, "v": (j * 7) % 13})
    ctx.write(out)


@pipe.stage(reads=build_rows)
def features(ctx):
    ctx.write([{**r, "f": r["v"] * 2} for r in ctx.input()])


@pipe.stage(reads=features, target_rows=10_000_000)
def summary(ctx):
    rows = ctx.input()
    ctx.write([{"count": len(rows), "sum_f": sum(r["f"] for r in rows)}])
