"""End-to-end tests on the local backend — no AWS, no pandas required."""

import json

import pytest

from examples.zz.pipeline import SIZES, build_pipeline
from gleanflow.context import _read_parts
from gleanflow.store import store_from_config


def _records(store, prefix):
    out = []
    for k in store.list(prefix):
        if "/part-" in k:
            out.extend(_read_parts(store, k))
    return out


def _expected():
    count = sum(SIZES.values())
    sum_f = sum(((j * 7) % 13) * 2 for n in SIZES.values() for j in range(n))
    return count, sum_f


def test_end_to_end_local(tmp_path):
    pipe = build_pipeline(s3_root=str(tmp_path))
    pipe.run(backend="local")
    store = store_from_config(pipe.config)

    summary = _records(store, "data/summary/")
    assert len(summary) == 1
    count, sum_f = _expected()
    assert summary[0]["count"] == count
    assert summary[0]["sum_f"] == sum_f


def test_decoupling_one_worker_many_tasks(tmp_path):
    """workers=1 must still finish all 7 build tasks — one worker, many tasks."""
    a = build_pipeline(s3_root=str(tmp_path / "a"))
    a.run(backend="local", workers=1)
    store = store_from_config(a.config)

    # every build_rows chunk completed...
    markers = [json.loads(store.get_bytes(k)) for k in store.list("results/build_rows/")
               if k.endswith(".json")]
    assert len(markers) == len(SIZES)
    # ...all by a single worker
    workers = {m.get("worker") for m in markers}
    assert workers == {"w0"}


def test_worker_count_does_not_change_result(tmp_path):
    r1 = build_pipeline(s3_root=str(tmp_path / "w1"))
    r1.run(backend="local", workers=1)
    r8 = build_pipeline(s3_root=str(tmp_path / "w8"))
    r8.run(backend="local", workers=8)

    s1 = _records(store_from_config(r1.config), "data/summary/")[0]
    s8 = _records(store_from_config(r8.config), "data/summary/")[0]
    assert s1 == s8


def test_resume_skips_done(tmp_path):
    build_pipeline(s3_root=str(tmp_path)).run(backend="local")
    snap = build_pipeline(s3_root=str(tmp_path)).run(backend="local")

    by_name = {st["name"]: st["counts"] for st in snap["stages"]}
    assert by_name["build_rows"].get("skipped") == len(SIZES)
    assert by_name["build_rows"].get("success", 0) == 0


def test_smoke_gate_aborts_on_worst_chunk(tmp_path):
    from gleanflow import Pipeline, PipelineConfig
    from gleanflow.controller import SmokeFailed, StageFailed

    pipe = Pipeline("boom", PipelineConfig(s3_root=str(tmp_path), max_redeliveries=1))
    days = pipe.source("days", chunks=[
        {"id": f"d{n}", "params": {"n": n}, "weight": float(n)} for n in (3, 80, 5)
    ])

    @pipe.stage(reads=days)
    def build(ctx):
        m = ctx.input()[0]
        if m["n"] == 80:          # the heaviest chunk -> smoke test should catch it
            raise ValueError("OOM on worst chunk")
        ctx.write([{"n": m["n"]}])

    with pytest.raises((SmokeFailed, StageFailed)):
        pipe.run(backend="local")
