# gleanflow worker image — modeled on invest-zigzag's CodeBuild image.
# ECR-Public AL2023 base (NOT Docker Hub) to dodge anonymous-pull rate limits in CodeBuild; pinned by
# digest so the dnf+pip layers stay cache-stable. Built ARM64 (see infra buildspec / job def).
# The pipeline's own code ships at runtime via APPCODE_S3 — this image is generic (deps + gleanflow core).
FROM public.ecr.aws/amazonlinux/amazonlinux:2023@sha256:d555bba4593ec3992e9ac8c081bad81d7f01a951bf0f890c7ae8f0026f381352

RUN dnf install -y python3.11 python3.11-pip awscli tar gzip unzip && dnf clean all \
    && ln -sf /usr/bin/python3.11 /usr/local/bin/python3

# heavier deps baked in; xgboost pinned 1.7.6 (CPU-only) by the per-pipeline EXTRA_PIP below
RUN python3.11 -m pip install --no-cache-dir boto3 pyarrow pandas

# per-pipeline extra deps, injected by the CodeBuild build-arg (gleanflow.infra: cfg.extra["image_pip"])
ARG EXTRA_PIP=""
RUN if [ -n "$EXTRA_PIP" ]; then python3.11 -m pip install --no-cache-dir $EXTRA_PIP; fi

COPY gleanflow /opt/gleanflow/gleanflow
ENV PYTHONPATH=/opt/gleanflow

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
