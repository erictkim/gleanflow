"""Polars map→reduce demo — shows how one lazy query breaks into chunks.

A logical query like

    scan_parquet("events/*.parquet")
      .filter(pl.col("amount") > 0)
      .group_by("user")
      .agg(hits=count, spend=sum(amount), avg=mean(amount))

is too big for one box when the dataset is many large files. gleanflow runs it as:

    source files ──pack by size──▶ MAP tasks (per-partition group_by/agg → partial)
                                          └────────────────▶ REDUCE task (combine partials)

Run it:

    python -m examples.polars_demo            # prints the chunk plan, then executes
    python -m examples.polars_demo --viz      # also opens the dashboard

No AWS needed — everything runs on the local backend against a temp directory.
"""

from __future__ import annotations

import os
import shutil
import sys

import polars as pl

from gleanflow import Pipeline, PipelineConfig
from gleanflow.controller import Controller
from gleanflow.polars import groupby_agg, map_reduce, parquet_source
from gleanflow.store import store_from_config

DEMO_DIR = os.path.abspath("./.gleanflow-polars-demo")
INPUT_DIR = os.path.join(DEMO_DIR, "events")

# deliberately uneven partition sizes (rows per file) -> packing must balance by bytes
FILE_ROWS = [50, 400, 80, 600, 120, 30, 300, 90]


def make_inputs() -> list[str]:
    """Write N parquet 'event' files with skewed sizes; return their paths."""
    shutil.rmtree(DEMO_DIR, ignore_errors=True)
    os.makedirs(INPUT_DIR, exist_ok=True)
    paths = []
    for i, n in enumerate(FILE_ROWS):
        rows = [{
            "user": f"u{(i * 7 + j) % 5}",
            "region": ["us", "eu", "apac"][(i + j) % 3],
            "amount": float((j * 13 + i * 5) % 97),
        } for j in range(n)]
        p = os.path.join(INPUT_DIR, f"events-{i:02d}.parquet")
        pl.DataFrame(rows).write_parquet(p)
        paths.append(p)
    return paths


def build_pipeline(paths: list[str], target_bytes: int) -> Pipeline:
    pipe = Pipeline("polars_demo", PipelineConfig(s3_root=os.path.join(DEMO_DIR, "out")))
    events = parquet_source(pipe, "events", paths)

    # combinable group-by/agg — reduce is auto-derived (sum/count/mean)
    groupby_agg(
        pipe, "by_user", source=events, by="user",
        aggs=[("hits", "count", None), ("spend", "sum", "amount"), ("avg", "mean", "amount")],
        where=pl.col("amount") > 0,
        target_bytes=target_bytes,          # how many files pack into one MAP task
    )

    # explicit map→reduce — top spenders per region
    map_reduce(
        pipe, "region_spend", source=events,
        map=lambda lf: lf.group_by("region").agg(pl.col("amount").sum().alias("spend")),
        reduce=lambda df: df.group_by("region").agg(pl.col("spend").sum()).sort("spend", descending=True),
        target_bytes=target_bytes,
    )
    return pipe


def explain(pipe: Pipeline) -> None:
    """Plan (without executing the compute) and print the chunk breakdown."""
    ctrl = Controller(pipe, backend="local")
    print("\nPHYSICAL PLAN — how the query is chunked\n" + "=" * 48)
    for st in pipe.topo_order():
        tasks = ctrl._plan(st, {})
        kind = "MAP   " if st.upstream_kind == "source" else "REDUCE"
        print(f"\n[{kind}] stage '{st.name}'  ->  {len(tasks)} task(s)"
              f"   (reads '{st.upstream_name}')")
        for t in tasks:
            members = t.params.get("_in", {}).get("members", [])
            if st.upstream_kind == "source":
                files = [os.path.basename(m["params"]["path"]) for m in members]
                print(f"    {t.key:22s} weight={int(t.weight):>7}B  files={files}")
            else:
                ids = [m["id"] for m in members]
                print(f"    {t.key:22s} consumes partials={ids}")


def main() -> None:
    viz = "--viz" in sys.argv
    paths = make_inputs()
    total = sum(os.path.getsize(p) for p in paths)
    target = total // 3                     # aim for ~3 map tasks

    print("LOGICAL QUERY")
    print("  pl.scan_parquet('events/*.parquet')")
    print("    .filter(pl.col('amount') > 0)")
    print("    .group_by('user').agg(hits=count, spend=sum(amount), avg=mean(amount))")
    print(f"\nINPUT: {len(paths)} parquet files, {total/1024:.1f} KB total "
          f"(rows per file: {FILE_ROWS})")
    print(f"PACK TARGET: ~{target} bytes per MAP task")

    pipe = build_pipeline(paths, target_bytes=target)
    explain(pipe)

    print("\nEXECUTING on local backend...\n" + "=" * 48)
    pipe.run(backend="local", viz=viz)

    store = store_from_config(pipe.config)
    for stage in ("by_user_reduce", "region_spend_reduce"):
        for k in store.list(f"data/{stage}/"):
            if k.endswith(".parquet"):
                import io
                df = pl.read_parquet(io.BytesIO(store.get_bytes(k)))
                print(f"\nRESULT  {stage}:")
                print(df.sort(df.columns[0]))

    # correctness: compare by_user against a single-shot query over all files
    single = (pl.scan_parquet(paths).filter(pl.col("amount") > 0)
              .group_by("user")
              .agg([pl.len().alias("hits"), pl.col("amount").sum().alias("spend"),
                    pl.col("amount").mean().alias("avg")])
              .collect().sort("user"))
    got = pl.read_parquet(io.BytesIO(store.get_bytes(
        next(k for k in store.list("data/by_user_reduce/") if k.endswith(".parquet"))))).sort("user")
    match = (got.select(["user", "hits", "spend"]).to_dicts()
             == single.select(["user", "hits", "spend"]).to_dicts())
    print(f"\nmatches single-shot query: {match}")


if __name__ == "__main__":
    main()
