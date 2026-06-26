# awsdemo — gleanflow on real AWS Batch

A self-contained gleanflow pipeline (`days → build_rows → features → summary`) that was
provisioned and run end-to-end on **AWS Batch (Fargate Spot)**. Kept as its own
top-level package so the runtime appcode zip (shipped to workers via `APPCODE_S3`)
imports cleanly as `awsdemo.pipeline:pipe` inside the container.

## Result (verified run)

- Output **identical to the local backend**: `{count: 188, sum_f: 2158}` (written as
  Parquet to S3 by the Fargate workers).
- **Decoupling proven on real infra**: `15 tasks ran on 3 Batch worker jobs` — one job
  drained ~5 tasks. Worker count ≠ task count; the fleet is sized independently of the
  task graph.
- Resilience: ~18 Batch job failures (early Local-Zone rejects + Spot churn) were
  absorbed by lease redelivery + retries; the pipeline still finished correct.

## Reproduce

Prereqs: AWS creds, Docker, Terraform on PATH. Region `us-east-1`.

```bash
# 0. tell the demo your account (no account ids are hardcoded in source)
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 1. provision infra (S3, SQS+DLQ, Batch Fargate-Spot CE @ vcpu=2, queue, ECR, IAM)
gleanflow infra apply awsdemo.pipeline:pipe          # terraform apply

# 2. build + push the worker image (amd64 for Fargate)
ECR=$(python3 -c "from awsdemo.pipeline import IMAGE; print(IMAGE)")
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "${ECR%/*}"
docker build --platform linux/amd64 -f gleanflow/infra/Dockerfile -t "$ECR" .
docker push "$ECR"

# 3. run on Batch — controller enqueues to SQS, workers poll + drain, markers gate stages
gleanflow run awsdemo.pipeline:pipe --backend aws --workers 4

# 4. tear down
gleanflow infra destroy awsdemo.pipeline:pipe
```

`bucket_suffix` in the pipeline config pins this deployment to its provisioned S3 bucket;
omit it for a fresh deploy (the suffix is then derived deterministically from the
pipeline name).

## What the run exercises

- **One generic worker image** runs every stage; the stage to run is decided per task
  from the SQS message — no per-stage job definitions.
- **Runtime code delivery**: editing `pipeline.py` needs no image rebuild — the package
  is re-zipped (content-hashed) to `APPCODE_S3` each run and unzipped onto the worker's
  `PYTHONPATH` ahead of the baked `gleanflow` core.
- **Self-scaling fleet**: the controller submits worker jobs to match SQS depth (up to
  `--workers`); idle workers self-terminate, so the fleet drains to zero between stages.
- **Idempotent resume**: `results/<key>.json` markers in S3 skip already-done chunks on
  re-run.

## Notes / gotchas surfaced while deploying

- **Local Zones**: the default VPC may include Local-Zone subnets where Fargate cannot
  run. The infra restricts the compute environment to `default-for-az=true` (standard
  AZs) — see `gleanflow/infra/main.tf.tmpl`.
- **Deterministic bucket name**: bucket suffix is derived with `hashlib` (not the
  per-process-randomized builtin `hash()`), so re-rendering never renames the bucket.
