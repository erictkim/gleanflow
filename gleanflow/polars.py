"""Polars map→reduce: break a lazy query over many files into small gleanflow tasks.

A single ``pl.LazyFrame`` scan over a large dataset won't fit one box. Here the scan is
split by file (or packed groups of files) into **map** tasks — each scans its partition
lazily, applies your per-partition transform, and writes a small partial — followed by a
single **reduce** task that combines the partials. Memory per task is bounded by the
partition size, not the whole dataset.

    src = parquet_source(pipe, "events", glob("s3-export/*.parquet"))

    # explicit map + reduce
    map_reduce(pipe, "top_paths",
        source=src,
        map=lambda lf: lf.filter(pl.col("status") == 500).group_by("path").len(),
        reduce=lambda df: df.group_by("path").agg(pl.col("len").sum()).sort("len", descending=True),
        target_bytes=512_000_000)            # pack ~512MB of files per map task

    # or a combinable group-by/agg with the reduce auto-derived
    groupby_agg(pipe, "by_user",
        source=src, by="user",
        aggs=[("hits", "count", None), ("spend", "sum", "amount"), ("avg", "mean", "amount")])

Both register two stages (``<name>`` map + ``<name>_reduce``) and return the reduce
Stage, so downstream stages can ``reads=`` it like any other.
"""

from __future__ import annotations

import io
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# source: a set of parquet files -> gleanflow Source (one chunk per file)
# ---------------------------------------------------------------------------
def parquet_source(pipe, name: str, paths, *, weights: Optional[list] = None):
    """A gleanflow Source whose chunks are parquet files (weight = file size).

    The map stage may pack several files per task via ``target_bytes`` — weight in
    bytes makes the packer balance tasks by data volume.
    """
    import os
    chunks = []
    for i, p in enumerate(paths):
        if weights is not None:
            w = float(weights[i])
        else:
            try:
                w = float(os.path.getsize(p))
            except OSError:
                w = 1.0
        chunks.append({"id": f"f{i}", "params": {"path": str(p)}, "weight": w})
    return pipe.source(name, chunks=chunks)


# ---------------------------------------------------------------------------
# polars IO bound to a task's slice (helpers used by the generated stages)
# ---------------------------------------------------------------------------
def scan_source(ctx):
    """Lazily scan the parquet files this map task owns -> pl.LazyFrame."""
    import polars as pl
    paths = [m["path"] for m in ctx.members]
    return pl.scan_parquet(paths)


def read_partials(ctx):
    """Read every upstream map partial for this reduce task -> one pl.DataFrame."""
    import polars as pl
    info = ctx.params.get("_in", {})
    frames = []
    for m in info.get("members", []):
        for k in ctx.store.list(f"data/{info['name']}/{m['id']}/"):
            if k.endswith(".parquet"):
                frames.append(pl.read_parquet(io.BytesIO(ctx.store.get_bytes(k))))
    return pl.concat(frames) if frames else pl.DataFrame()


def write_frame(ctx, df) -> None:
    """Write a pl.DataFrame as this task's parquet output part."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    ctx.write_bytes("part-00000.parquet", buf.getvalue())


# ---------------------------------------------------------------------------
# general map -> reduce
# ---------------------------------------------------------------------------
def map_reduce(pipe, name: str, *, source, map: Callable, reduce: Callable, **map_stage_kw):
    """Register a map stage (per-partition ``map``) + a reduce stage (``reduce``).

    ``map(lf: LazyFrame) -> LazyFrame | DataFrame`` runs on each partition.
    ``reduce(df: DataFrame) -> DataFrame`` runs once over the concatenated partials.
    ``map_stage_kw`` (e.g. ``target_bytes=...``) tunes how files are packed per task.
    """
    def _map(ctx):
        import polars as pl
        res = map(scan_source(ctx))
        df = res.collect() if isinstance(res, pl.LazyFrame) else res
        write_frame(ctx, df)

    map_stage = pipe.stage(reads=source, name=name, **map_stage_kw)(_map)

    def _reduce(ctx):
        write_frame(ctx, reduce(read_partials(ctx)))

    # huge target -> every partial lands in a single reduce task
    return pipe.stage(reads=map_stage, name=f"{name}_reduce", target_rows=10 ** 15)(_reduce)


# ---------------------------------------------------------------------------
# combinable group-by / agg with an auto-derived reduce
# ---------------------------------------------------------------------------
def _compile_aggs(aggs):
    """(name, op, col) specs -> (map_exprs, reduce_exprs, post, final_cols).

    Only *combinable* aggregations: a partial agg per partition then a second agg
    over the partials yields the global answer. mean is split into sum + count and
    divided at the end.
    """
    import polars as pl
    map_exprs, reduce_exprs, post, final = [], [], [], []
    for name, op, col in aggs:
        if op == "sum":
            map_exprs.append(pl.col(col).sum().alias(name))
            reduce_exprs.append(pl.col(name).sum().alias(name))
        elif op == "min":
            map_exprs.append(pl.col(col).min().alias(name))
            reduce_exprs.append(pl.col(name).min().alias(name))
        elif op == "max":
            map_exprs.append(pl.col(col).max().alias(name))
            reduce_exprs.append(pl.col(name).max().alias(name))
        elif op == "count":
            map_exprs.append(pl.len().alias(name))
            reduce_exprs.append(pl.col(name).sum().alias(name))
        elif op == "mean":
            s, n = f"{name}__s", f"{name}__n"
            map_exprs += [pl.col(col).sum().alias(s), pl.len().alias(n)]
            reduce_exprs += [pl.col(s).sum().alias(s), pl.col(n).sum().alias(n)]
            post.append((pl.col(s) / pl.col(n)).alias(name))
        else:
            raise ValueError(f"non-combinable agg op: {op!r} "
                             f"(use sum/min/max/count/mean, or map_reduce)")
        final.append(name)
    return map_exprs, reduce_exprs, post, final


def groupby_agg(pipe, name: str, *, source, by, aggs, where=None, **stage_kw):
    """A distributed ``group_by(by).agg(aggs)`` as map→reduce.

    ``by``   : group key (str or list of str).
    ``aggs`` : list of ``(out_name, op, col)`` where op ∈ sum|min|max|count|mean
               (``col`` ignored for count). The reduce is derived automatically.
    ``where``: optional ``pl.Expr`` filter applied before the per-partition agg.
    """
    by_list = [by] if isinstance(by, str) else list(by)
    map_exprs, reduce_exprs, post, final = _compile_aggs(aggs)

    def _map(lf):
        if where is not None:
            lf = lf.filter(where)
        return lf.group_by(by_list).agg(map_exprs)

    def _reduce(df):
        out = df.group_by(by_list).agg(reduce_exprs)
        if post:
            out = out.with_columns(post)
        return out.select(by_list + final)

    return map_reduce(pipe, name, source=source, map=_map, reduce=_reduce, **stage_kw)
