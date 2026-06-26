"""The inter-stage data bus: a key->bytes object store.

Two interchangeable implementations behind one interface so stage code (and the
worker) never knows whether it is talking to S3 or the local filesystem:

  * ``LocalStore``  — a base directory; used by the ``local`` backend, tests, dev.
  * ``S3Store``     — real S3 (boto3, imported lazily); used by the ``aws`` backend.

Both expose the same ``put/get/exists/list`` plus a ``get_file`` that mirrors an
object into a **local disk cache** (skip-if-present), which is how a worker reuses
a slice across tasks and how the next local stage reuses the previous one's output.
"""

from __future__ import annotations

import os
import shutil
from typing import Optional


def split_s3(uri: str) -> tuple[str, str]:
    """``s3://bucket/a/b`` -> ``("bucket", "a/b")``."""
    assert uri.startswith("s3://"), uri
    bucket, _, key = uri[5:].partition("/")
    return bucket, key.rstrip("/")


class Store:
    """Key-addressed object store. Keys are relative paths under a root."""

    cache_dir: str

    def put_bytes(self, key: str, data: bytes) -> None: ...
    def get_bytes(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...
    def list(self, prefix: str) -> list[str]: ...

    def put_file(self, key: str, local_path: str) -> None:
        with open(local_path, "rb") as f:
            self.put_bytes(key, f.read())

    def get_file(self, key: str, local_path: Optional[str] = None) -> str:
        """Download ``key`` to a local path (cached). Returns the local path."""
        local_path = local_path or os.path.join(self.cache_dir, key)
        if os.path.exists(local_path):
            return local_path
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        tmp = local_path + ".part"
        with open(tmp, "wb") as f:
            f.write(self.get_bytes(key))
        os.replace(tmp, local_path)
        return local_path


class LocalStore(Store):
    def __init__(self, root: str, cache_dir: Optional[str] = None):
        self.root = os.path.abspath(root[7:] if root.startswith("file://") else root)
        os.makedirs(self.root, exist_ok=True)
        self.cache_dir = cache_dir or os.path.join(self.root, "_cache")

    def _p(self, key: str) -> str:
        return os.path.join(self.root, key)

    def put_bytes(self, key: str, data: bytes) -> None:
        p = self._p(key)
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        tmp = p + ".part"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, p)

    def get_bytes(self, key: str) -> bytes:
        with open(self._p(key), "rb") as f:
            return f.read()

    def exists(self, key: str) -> bool:
        return os.path.exists(self._p(key))

    def list(self, prefix: str) -> list[str]:
        base = self._p(prefix)
        root = base if os.path.isdir(base) else os.path.dirname(base)
        out: list[str] = []
        for dirpath, _, names in os.walk(root):
            for n in names:
                full = os.path.join(dirpath, n)
                rel = os.path.relpath(full, self.root)
                if rel.startswith(prefix) and not rel.endswith(".part"):
                    out.append(rel)
        return sorted(out)

    # local store already has the bytes on disk; cache = mirror via copy
    def get_file(self, key: str, local_path: Optional[str] = None) -> str:
        if local_path is None:
            return self._p(key)
        if not os.path.exists(local_path):
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            shutil.copyfile(self._p(key), local_path)
        return local_path


class S3Store(Store):
    def __init__(self, root: str, region: str = "us-east-1", cache_dir: str = "/tmp/gleanflow"):
        import boto3  # lazy: core has no boto3 dependency
        self.bucket, self.prefix = split_s3(root)
        self.s3 = boto3.client("s3", region_name=region)
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _k(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)

    def get_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=self._k(key))["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self.s3.head_object(Bucket=self.bucket, Key=self._k(key))
            return True
        except ClientError:
            return False

    def list(self, prefix: str) -> list[str]:
        out: list[str] = []
        token = None
        base = self._k(prefix)
        while True:
            kw = {"Bucket": self.bucket, "Prefix": base}
            if token:
                kw["ContinuationToken"] = token
            r = self.s3.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                k = o["Key"]
                out.append(k[len(self.prefix) + 1:] if self.prefix else k)
            if not r.get("IsTruncated"):
                break
            token = r["NextContinuationToken"]
        return out


def store_from_config(cfg) -> Store:
    if cfg.is_s3:
        return S3Store(cfg.s3_root, region=cfg.region)
    return LocalStore(cfg.s3_root)


class PartWriter:
    """Stream records to row-capped part files (bounded memory writes).

    Rolls a new part every ``cap_rows`` and uploads it to the store as it goes, so
    even a huge chunk never materializes fully in RAM. Writes Parquet when pyarrow
    is available, else newline-delimited JSON. Returns the written part keys.
    """

    def __init__(self, store: Store, prefix: str, cap_rows: int = 1_000_000):
        self.store = store
        self.prefix = prefix.rstrip("/")
        self.cap = cap_rows
        self._buf: list[dict] = []
        self._part = 0
        self.keys: list[str] = []
        self._fmt = "parquet" if _have_pyarrow() else "jsonl"

    def write(self, records) -> None:
        if isinstance(records, dict):
            records = [records]
        for r in records:
            self._buf.append(r)
            if len(self._buf) >= self.cap:
                self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        key = f"{self.prefix}/part-{self._part:05d}.{self._fmt}"
        self.store.put_bytes(key, _encode(self._buf, self._fmt))
        self.keys.append(key)
        self._part += 1
        self._buf = []

    def close(self) -> list[str]:
        self._flush()
        return self.keys


def _have_pyarrow() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except Exception:
        return False


def _encode(records: list[dict], fmt: str) -> bytes:
    if fmt == "parquet":
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()
    import json
    return ("\n".join(json.dumps(r, separators=(",", ":")) for r in records)).encode()
