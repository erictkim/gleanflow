"""AWS Batch fleet — long-lived worker jobs that poll the SQS task queue.

``ensure(n)`` submits enough generic *worker* jobs (one job def, sized via
``containerOverrides``) so ~``n`` are running; each runs ``python -m gleanflow.worker``
and drains many tasks before self-terminating on idle. Scale-down is implicit (idle
exit), so this only ever submits to top up — the Fargate-Spot compute environment
provides the elastic instances underneath. User code is shipped at runtime as a
content-hashed zip to ``APPCODE_S3`` (no image rebuild for code changes).
"""

from __future__ import annotations

import hashlib
import io
import os
import zipfile
from typing import Optional


class BatchFleet:
    def __init__(self, cfg, *, pipeline_spec: str, resources: Optional[dict] = None):
        import boto3
        self.cfg = cfg
        self.spec = pipeline_spec               # "module:attr" the worker imports
        self.resources = resources or {"vcpu": cfg.default_vcpu, "mem": cfg.default_mem}
        self.batch = boto3.client("batch", region_name=cfg.region)
        self._job_ids: list[str] = []
        self._appcode_s3 = ""

    # ---- runtime code delivery -------------------------------------------
    def deliver_code(self, package_dir: str) -> str:
        """Zip the user package, content-hash it, upload to APPCODE_S3, return the key."""
        from ..store import store_from_config
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(package_dir):
                for f in files:
                    if f.endswith((".pyc",)) or "__pycache__" in root:
                        continue
                    full = os.path.join(root, f)
                    z.write(full, os.path.relpath(full, os.path.dirname(package_dir)))
        data = buf.getvalue()
        sha = hashlib.sha1(data).hexdigest()[:12]
        store = store_from_config(self.cfg)
        key = f"lib/appcode-{sha}.zip"
        store.put_bytes(key, data)
        self._appcode_s3 = f"{self.cfg.s3_root}/{key}"
        return self._appcode_s3

    # ---- fleet -----------------------------------------------------------
    def _submit_one(self, idx: int) -> str:
        env = [
            {"name": "GLEANFLOW_PIPELINE", "value": self.spec},
            {"name": "OUT_S3", "value": self.cfg.s3_root},
            {"name": "SQS_URL", "value": self.cfg.sqs_url},
            {"name": "AWS_REGION", "value": self.cfg.region},
            {"name": "OMP_NUM_THREADS", "value": str(self.resources.get("omp", self.resources["vcpu"]))},
        ]
        if self._appcode_s3:
            env.append({"name": "APPCODE_S3", "value": self._appcode_s3})
        ov = {
            "environment": env,
            "resourceRequirements": [
                {"type": "VCPU", "value": str(self.resources["vcpu"])},
                {"type": "MEMORY", "value": str(self.resources["mem"])},
            ],
        }
        r = self.batch.submit_job(
            jobName=f"gleanflow-worker-{idx}"[:128],
            jobQueue=self.cfg.queue_arn, jobDefinition=self.cfg.job_def,
            containerOverrides=ov, retryStrategy={"attempts": 2},
        )
        return r["jobId"]

    def ensure(self, n: int) -> None:
        n = min(n, self.cfg.max_workers)
        live = self.running()
        for i in range(live, n):
            self._job_ids.append(self._submit_one(i))

    def running(self) -> int:
        if not self._job_ids:
            return 0
        live = 0
        for i in range(0, len(self._job_ids), 100):
            batch = self._job_ids[i:i + 100]
            for j in self.batch.describe_jobs(jobs=batch)["jobs"]:
                if j["status"] in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
                    live += 1
        return live

    def drain(self) -> None:
        # workers self-terminate on idle; nothing to actively kill
        return
