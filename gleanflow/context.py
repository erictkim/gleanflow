"""``Ctx`` — the only object a user's stage function touches.

It hides the implementation details the library owns: where my slice lives, how to
download it (with local cache), how to stream output back to the store under bounded
memory, and how to checkpoint. A stage author writes ``ctx.input()`` /
``ctx.write(...)`` and never sees S3 keys, the queue, or the fleet.
"""

from __future__ import annotations

import json
import os
from typing import Optional

from .ckpt import Checkpoint
from .store import PartWriter, Store


class Ctx:
    def __init__(self, pipe, stage, task, store: Store):
        self.pipe = pipe
        self.stage = stage
        self.task = task
        self.store = store
        self.config = pipe.config
        self.params = task.params
        self._writer: Optional[PartWriter] = None
        self._outputs: list[str] = []

    # ---- identity ---------------------------------------------------------
    @property
    def chunk_id(self) -> str:
        return self.params.get("_chunk_id", "c0")

    @property
    def members(self) -> list[dict]:
        """The upstream chunk params this task owns (its slice of work)."""
        return [m.get("params", {}) for m in self.params.get("_in", {}).get("members", [])]

    @property
    def out_prefix(self) -> str:
        return f"data/{self.stage.name}/{self.chunk_id}"

    @property
    def cache_dir(self) -> str:
        d = os.path.join(getattr(self.store, "cache_dir", "/tmp/gleanflow"), "work", self.task.key)
        os.makedirs(d, exist_ok=True)
        return d

    # ---- input ------------------------------------------------------------
    def input(self) -> list[dict]:
        """Load this task's slice as a list of records.

        * source upstream -> the member params (or ``source.reader`` output)
        * stage  upstream -> the upstream stage's output records for my members
        """
        info = self.params.get("_in")
        if not info:
            return self.members
        if info["kind"] == "source":
            src = self.pipe.sources[info["name"]]
            if src.reader is not None:
                out: list[dict] = []
                for m in info["members"]:
                    out.extend(src.reader(self.config, m.get("params", {})))
                return out
            return [m.get("params", {}) for m in info["members"]]
        # stage upstream
        out = []
        for m in info["members"]:
            for k in self.store.list(f"data/{info['name']}/{m['id']}/"):
                out.extend(_read_parts(self.store, k))
        return out

    def input_df(self):
        import pandas as pd
        return pd.DataFrame(self.input())

    def aux(self, key: str) -> bytes:
        """Load an auxiliary object (model, prior-period output, ...) by store key."""
        return self.store.get_bytes(key)

    # ---- output -----------------------------------------------------------
    def _w(self) -> PartWriter:
        if self._writer is None:
            self._writer = PartWriter(self.store, self.out_prefix, cap_rows=self.stage.cap_rows)
        return self._writer

    def write(self, records) -> None:
        """Append records (list[dict] or a pandas DataFrame) to this chunk's output."""
        if hasattr(records, "to_dict"):  # DataFrame
            records = records.to_dict("records")
        self._w().write(records)

    def write_bytes(self, name: str, data: bytes) -> str:
        key = f"{self.out_prefix}/{name}"
        self.store.put_bytes(key, data)
        self._outputs.append(key)
        return key

    def write_model(self, clf, name: str = "model.json") -> str:
        local = os.path.join(self.cache_dir, name)
        if hasattr(clf, "save_model"):
            clf.save_model(local)
        else:
            with open(local, "w") as f:
                json.dump(clf, f)
        with open(local, "rb") as f:
            return self.write_bytes(name, f.read())

    @property
    def checkpoint(self) -> Checkpoint:
        return Checkpoint(self.store, self.task.key, work_dir=os.path.join(self.cache_dir, "ckpt"))

    # ---- lifecycle (called by the worker) ---------------------------------
    def finalize(self) -> list[str]:
        if self._writer is not None:
            self._outputs.extend(self._writer.close())
        return self._outputs


def _read_parts(store: Store, key: str) -> list[dict]:
    data = store.get_bytes(key)
    if key.endswith(".parquet"):
        import io
        import pyarrow.parquet as pq
        return pq.read_table(io.BytesIO(data)).to_pylist()
    return [json.loads(line) for line in data.decode().splitlines() if line.strip()]
