#!/usr/bin/env bash
# Container entrypoint for a gleanflow worker job.
# Pulls runtime user code (APPCODE_S3) so code changes need no image rebuild,
# tees logs to the object store for post-mortem, then runs the worker poll loop.
set -euo pipefail

# capture logs to S3 on exit (best effort)
LOG=/tmp/run.log
exec > >(stdbuf -oL tee "$LOG") 2>&1
trap 'rc=$?; [ -n "${OUT_S3:-}" ] && aws s3 cp "$LOG" "${OUT_S3}/_debug/${AWS_BATCH_JOB_ID:-x}.log" || true; exit $rc' EXIT

# thread caps so a worker does not oversubscribe its vCPU allotment
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="$OMP_NUM_THREADS"
export MKL_NUM_THREADS="$OMP_NUM_THREADS"
export NUMEXPR_NUM_THREADS="$OMP_NUM_THREADS"

# runtime code delivery
if [ -n "${APPCODE_S3:-}" ]; then
  aws s3 cp "$APPCODE_S3" /tmp/appcode.zip
  mkdir -p /opt/appcode && (cd /opt/appcode && unzip -oq /tmp/appcode.zip)
  export PYTHONPATH="/opt/appcode:${PYTHONPATH:-}"
fi

exec python3 -m gleanflow.worker
