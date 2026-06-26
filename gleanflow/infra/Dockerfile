# gleanflow worker image. One generic image runs every stage — the stage to run
# is decided per task at runtime (PARAMS on the queue), and user code is delivered
# at runtime via APPCODE_S3, so this rarely needs rebuilding.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        awscli unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# heavier deps that warrant baking in (override for your workload)
RUN pip install --no-cache-dir boto3 pyarrow pandas

COPY gleanflow /opt/gleanflow/gleanflow
ENV PYTHONPATH=/opt/gleanflow

COPY gleanflow/infra/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
