"""Example gleanflow pipeline: a fan-out / fan-in DAG with deliberately uneven chunks.

  days ─▶ build_rows ─▶ features ─▶ summary

``days`` is an external source with very uneven per-date sizes (3 .. 80 rows) — the
exact skew that OOMs a monolithic job. gleanflow runs each date as its own task
(heaviest-first smoke), ``features`` maps 1:1 over them, and ``summary`` packs every
feature chunk into a single reduce task. Runs fully locally — no AWS, no pandas.

    python -m examples.zz.pipeline --viz        # opens http://127.0.0.1:8765
    gleanflow run examples.zz.pipeline:pipe --viz
"""

from __future__ import annotations

from gleanflow import Pipeline, PipelineConfig

# date -> number of rows it produces (intentionally lumpy)
SIZES = {
    "20260501": 3, "20260502": 30, "20260503": 5, "20260504": 50,
    "20260505": 8, "20260506": 80, "20260507": 12,
}


def build_pipeline(s3_root: str = "./.gleanflow-demo", max_workers: int = 8) -> Pipeline:
    pipe = Pipeline("zzdemo", PipelineConfig(s3_root=s3_root, max_workers=max_workers))

    days = pipe.source("days", chunks=[
        {"id": f"date={d}", "params": {"date": d, "rows": n}, "weight": float(n)}
        for d, n in SIZES.items()
    ])

    @pipe.stage(reads=days)
    def build_rows(ctx):
        out = []
        for m in ctx.input():                      # member params: {date, rows}
            d, n = m["date"], m["rows"]
            for j in range(n):
                out.append({"date": d, "i": j, "v": (j * 7) % 13})
        ctx.write(out)

    @pipe.stage(reads=build_rows)
    def features(ctx):
        ctx.write([{**r, "f": r["v"] * 2} for r in ctx.input()])

    @pipe.stage(reads=features, target_rows=10_000_000)   # pack everything -> one reduce task
    def summary(ctx):
        rows = ctx.input()
        ctx.write([{"count": len(rows), "sum_f": sum(r["f"] for r in rows)}])

    return pipe


pipe = build_pipeline()


if __name__ == "__main__":
    import sys
    pipe.run(backend="local", viz=("--viz" in sys.argv))
