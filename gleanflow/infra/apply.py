"""Always-provision infra: render Terraform from the pipeline config and apply it.

``apply(pipe)`` renders ``main.tf.tmpl`` into a workdir, runs ``terraform init &&
apply``, and folds the outputs (s3 root, Batch queue/job-def, SQS urls, ECR) back
into a JSON sidecar the CLI loads into ``PipelineConfig``. ``destroy(pipe)`` tears it
down. Terraform must be on PATH; everything else is created here.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess

_HERE = os.path.dirname(__file__)


def _workdir(pipe) -> str:
    d = os.path.join(os.path.expanduser("~/.gleanflow"), pipe.name, "infra")
    os.makedirs(d, exist_ok=True)
    return d


def _render(pipe) -> str:
    cfg = pipe.config
    with open(os.path.join(_HERE, "main.tf.tmpl")) as f:
        tf = f.read()
    # deterministic across processes (builtin hash() is per-process randomized,
    # which would rename the bucket on every render and force a destroy/recreate).
    # ``bucket_suffix`` lets a deployment pin an existing bucket.
    suffix = str(cfg.extra.get("bucket_suffix")
                 or int(hashlib.sha1(pipe.name.encode()).hexdigest(), 16) % 100000)
    subs = {
        "%%PROJECT%%": pipe.name.replace("_", "-"),
        "%%REGION%%": cfg.region,
        "%%SUFFIX%%": suffix,
        "%%MAXVCPU%%": str(max(16, cfg.max_workers * cfg.default_vcpu)),
        "%%VCPU%%": str(cfg.default_vcpu),
        "%%MEM%%": str(cfg.default_mem),
        "%%LEASE%%": str(int(cfg.lease_seconds)),
        "%%MAXREDELIVER%%": str(cfg.max_redeliveries),
        "%%IMAGE%%": cfg.image or "PLACEHOLDER_PUSH_IMAGE_FIRST",
    }
    for k, v in subs.items():
        tf = tf.replace(k, v)
    return tf


def _terraform(workdir: str, *args: str) -> None:
    subprocess.run(["terraform", *args], cwd=workdir, check=True)


def apply(pipe, *, auto_approve: bool = True) -> dict:
    wd = _workdir(pipe)
    with open(os.path.join(wd, "main.tf"), "w") as f:
        f.write(_render(pipe))
    _terraform(wd, "init", "-input=false")
    args = ["apply", "-input=false"] + (["-auto-approve"] if auto_approve else [])
    _terraform(wd, *args)
    out = json.loads(subprocess.run(
        ["terraform", "output", "-json"], cwd=wd, check=True,
        capture_output=True, text=True).stdout)
    outputs = {k: v["value"] for k, v in out.items()}
    with open(os.path.join(wd, "outputs.json"), "w") as f:
        json.dump(outputs, f, indent=2)
    _apply_to_config(pipe, outputs)
    return outputs


def destroy(pipe, *, auto_approve: bool = True) -> None:
    wd = _workdir(pipe)
    args = ["destroy", "-input=false"] + (["-auto-approve"] if auto_approve else [])
    _terraform(wd, *args)


def load_outputs(pipe) -> dict:
    p = os.path.join(_workdir(pipe), "outputs.json")
    if not os.path.exists(p):
        return {}
    with open(p) as f:
        outputs = json.load(f)
    _apply_to_config(pipe, outputs)
    return outputs


def _apply_to_config(pipe, outputs: dict) -> None:
    cfg = pipe.config
    cfg.s3_root = outputs.get("s3_root", cfg.s3_root)
    cfg.queue_arn = outputs.get("queue", cfg.queue_arn)
    cfg.job_def = outputs.get("job_def", cfg.job_def)
    cfg.sqs_url = outputs.get("sqs_url", cfg.sqs_url)
    cfg.dlq_url = outputs.get("dlq_url", cfg.dlq_url)
