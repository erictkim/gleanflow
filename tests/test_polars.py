"""Polars map->reduce on the local backend, checked against a single-shot query."""

import pytest

pl = pytest.importorskip("polars")

from gleanflow import Pipeline, PipelineConfig
from gleanflow.polars import groupby_agg, map_reduce, parquet_source
from gleanflow.store import store_from_config


def _make_files(tmp_path, n=6, per=5):
    files, allrows = [], []
    for i in range(n):
        rows = [{"k": ["A", "B", "C"][j % 3], "v": i * 10 + j} for j in range(per)]
        p = tmp_path / f"in{i}.parquet"
        pl.DataFrame(rows).write_parquet(p)
        files.append(str(p))
        allrows.extend(rows)
    return files, pl.DataFrame(allrows)


def _read_result(pipe, stage):
    store = store_from_config(pipe.config)
    for k in store.list(f"data/{stage}/"):
        if k.endswith(".parquet"):
            import io
            return pl.read_parquet(io.BytesIO(store.get_bytes(k)))
    raise AssertionError("no result parquet")


def test_groupby_agg_matches_single_shot(tmp_path):
    files, full = _make_files(tmp_path, n=6)
    pipe = Pipeline("plagg", PipelineConfig(s3_root=str(tmp_path / "out")))
    src = parquet_source(pipe, "src", files)
    groupby_agg(pipe, "agg", source=src, by="k",
                aggs=[("total", "sum", "v"), ("rows", "count", None), ("avg", "mean", "v")])
    pipe.run(backend="local")

    got = _read_result(pipe, "agg_reduce").sort("k")
    exp = (full.group_by("k")
           .agg([pl.col("v").sum().alias("total"),
                 pl.len().alias("rows"),
                 pl.col("v").mean().alias("avg")])
           .sort("k").select(["k", "total", "rows", "avg"]))

    assert got.select(["k", "total", "rows"]).to_dicts() == exp.select(["k", "total", "rows"]).to_dicts()
    # mean compared with tolerance
    ga = dict(zip(got["k"], got["avg"]))
    ea = dict(zip(exp["k"], exp["avg"]))
    for k in ea:
        assert abs(ga[k] - ea[k]) < 1e-9


def test_groupby_agg_fans_out_one_task_per_file(tmp_path):
    files, _ = _make_files(tmp_path, n=6)
    pipe = Pipeline("planf", PipelineConfig(s3_root=str(tmp_path / "out")))
    src = parquet_source(pipe, "src", files)
    groupby_agg(pipe, "agg", source=src, by="k", aggs=[("total", "sum", "v")])
    pipe.run(backend="local")

    store = store_from_config(pipe.config)
    map_markers = [k for k in store.list("results/agg/") if k.endswith(".json")]
    reduce_markers = [k for k in store.list("results/agg_reduce/") if k.endswith(".json")]
    assert len(map_markers) == 6        # 6 files -> 6 map tasks
    assert len(reduce_markers) == 1     # all partials -> 1 reduce task


def test_packing_groups_files_per_task(tmp_path):
    files, full = _make_files(tmp_path, n=6)
    pipe = Pipeline("plpack", PipelineConfig(s3_root=str(tmp_path / "out")))
    src = parquet_source(pipe, "src", files, weights=[1, 1, 1, 1, 1, 1])
    # target weight 2 -> ~3 map tasks instead of 6
    groupby_agg(pipe, "agg", source=src, by="k",
                aggs=[("total", "sum", "v")], target_bytes=2)
    pipe.run(backend="local")

    store = store_from_config(pipe.config)
    map_markers = [k for k in store.list("results/agg/") if k.endswith(".json")]
    assert len(map_markers) == 3        # packed 2 files per task

    got = _read_result(pipe, "agg_reduce").sort("k")
    exp = full.group_by("k").agg(pl.col("v").sum().alias("total")).sort("k")
    assert dict(zip(got["k"], got["total"])) == dict(zip(exp["k"], exp["total"]))


def test_map_reduce_explicit(tmp_path):
    files, full = _make_files(tmp_path, n=6)
    pipe = Pipeline("plmr", PipelineConfig(s3_root=str(tmp_path / "out")))
    src = parquet_source(pipe, "src", files)
    map_reduce(pipe, "big",
               source=src,
               map=lambda lf: lf.filter(pl.col("v") > 20).group_by("k").agg(pl.col("v").sum().alias("s")),
               reduce=lambda df: df.group_by("k").agg(pl.col("s").sum().alias("s")))
    pipe.run(backend="local")

    got = _read_result(pipe, "big_reduce").sort("k")
    exp = (full.filter(pl.col("v") > 20).group_by("k")
           .agg(pl.col("v").sum().alias("s")).sort("k"))
    assert dict(zip(got["k"], got["s"])) == dict(zip(exp["k"], exp["s"]))
