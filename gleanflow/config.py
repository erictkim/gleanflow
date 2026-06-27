"""Pipeline configuration.

A single ``PipelineConfig`` carries everything the three planes need: where the
object store lives, which AWS resources to talk to, and how the worker fleet is
sized. The same config drives the ``local`` backend (no AWS) and the ``aws``
backend (SQS task queue + Batch worker fleet) — only ``s3_root`` and the resource
ARNs differ.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    # --- object store (inter-stage data bus) -------------------------------
    # ``s3://bucket/prefix`` -> S3 store; any other path -> local-filesystem store.
    s3_root: str = "./.gleanflow"

    # --- AWS wiring (only used by the aws backend) -------------------------
    region: str = "us-east-1"
    image: str = ""           # ECR image:tag the worker job def runs
    queue_arn: str = ""       # Batch job queue (where worker jobs land)
    job_def: str = ""         # generic worker job definition
    sqs_url: str = ""         # task queue
    dlq_url: str = ""         # dead-letter queue
    appcode_s3: str = ""      # content-hashed user-code zip prefix

    # --- fleet sizing (independent of task count) --------------------------
    max_workers: int = 16
    worker_idle_timeout: float = 60.0   # a worker exits after this many idle seconds
    lease_seconds: float = 300.0        # task lease / SQS visibility timeout
    heartbeat_seconds: float = 30.0     # how often a worker extends its lease
    max_redeliveries: int = 3           # -> DLQ after this many failed deliveries

    # --- defaults for stages without explicit resources -------------------
    default_vcpu: int = 2
    default_mem: int = 16384            # MB

    # --- viz ----------------------------------------------------------------
    viz_host: str = "127.0.0.1"
    viz_port: int = 8765

    # --- LLM failure agent --------------------------------------------------
    failure_policy: str = "off"        # "off" | "report" | "remediate"
    failure_handler: object = None     # FailureHandler callable; default agent if None
    max_remediations: int = 2          # cap on auto-fixes per stage

    # arbitrary extra knobs a user pipeline may read
    extra: dict = field(default_factory=dict)

    @property
    def is_s3(self) -> bool:
        return self.s3_root.startswith("s3://")
