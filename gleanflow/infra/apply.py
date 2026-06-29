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
_PKG = os.path.dirname(_HERE)          # the gleanflow package dir (baked into the worker image)


def _ctx_hash(cfg) -> str:
    """Content hash of the image build context (Dockerfile + entrypoint + gleanflow source + EXTRA_PIP).
    Drives the CodeBuild trigger so the generic image rebuilds only when it actually changes."""
    parts = [open(os.path.join(_HERE, "Dockerfile"), "rb").read(),
             open(os.path.join(_HERE, "entrypoint.sh"), "rb").read(),
             str(cfg.extra.get("image_pip", "")).encode()]
    for root, _, files in os.walk(_PKG):           # all baked gleanflow .py (recursive)
        if "__pycache__" in root:
            continue
        for f in sorted(files):
            if f.endswith(".py"):
                parts.append(open(os.path.join(root, f), "rb").read())
    return hashlib.sha1(b"".join(parts)).hexdigest()[:12]


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
        # in-cloud CodeBuild image build (no local docker)
        "%%EXTRA_PIP%%": str(cfg.extra.get("image_pip", "")),
        "%%GLEANFLOW_SRC%%": _PKG,
        "%%INFRA_DIR%%": _HERE,
        "%%CTX_HASH%%": _ctx_hash(cfg),
        # extra read-only source buckets for the worker job role (HCL list literal)
        "%%READ_ARNS%%": json.dumps([a for b in cfg.extra.get("read_buckets", [])
                                     for a in (f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*")]),
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
    wd = _workdir(pipe)
    p = os.path.join(wd, "outputs.json")
    if os.path.exists(p):
        with open(p) as f:
            outputs = json.load(f)
    else:
        # fall back to live terraform outputs (e.g. infra applied via raw `terraform apply`)
        try:
            raw = subprocess.run(["terraform", "output", "-json"], cwd=wd, check=True,
                                 capture_output=True, text=True).stdout
            outputs = {k: v["value"] for k, v in json.loads(raw).items()}
            with open(p, "w") as f:
                json.dump(outputs, f, indent=2)
        except Exception:
            return {}
    _apply_to_config(pipe, outputs)
    return outputs


def _apply_to_config(pipe, outputs: dict) -> None:
    cfg = pipe.config
    cfg.s3_root = outputs.get("s3_root", cfg.s3_root)
    cfg.queue_arn = outputs.get("queue", cfg.queue_arn)
    cfg.job_def = outputs.get("job_def", cfg.job_def)
    cfg.sqs_url = outputs.get("sqs_url", cfg.sqs_url)
    cfg.dlq_url = outputs.get("dlq_url", cfg.dlq_url)
