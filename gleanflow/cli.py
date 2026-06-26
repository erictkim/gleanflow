"""``gleanflow`` command-line entry point.

    gleanflow run    pkg.mod:pipe [--backend local|aws] [--workers N] [--viz] [--force] k=v...
    gleanflow status pkg.mod:pipe
    gleanflow infra  apply|destroy pkg.mod:pipe
    gleanflow worker pkg.mod:pipe            # run a single poll-loop worker locally
    gleanflow viz    snapshot.json [--port P]
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys


def _load(spec: str):
    """``pkg.mod:attr`` -> the Pipeline object (and its containing package dir)."""
    mod_name, _, attr = spec.partition(":")
    sys.path.insert(0, os.getcwd())
    mod = importlib.import_module(mod_name)
    pipe = getattr(mod, attr or "pipe")
    pkg_dir = os.path.dirname(os.path.abspath(mod.__file__))
    pipe.config.extra.setdefault("pipeline_spec", spec)
    pipe.config.extra.setdefault("package_dir", pkg_dir)
    return pipe


def _coerce(v: str):
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return v


def _kv(pairs: list[str]) -> dict:
    out = {}
    for p in pairs:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k] = _coerce(v)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="gleanflow")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run a pipeline")
    r.add_argument("spec")
    r.add_argument("--backend", default="local", choices=["local", "aws"])
    r.add_argument("--workers", type=int, default=None)
    r.add_argument("--viz", action="store_true")
    r.add_argument("--force", action="store_true")
    r.add_argument("--no-smoke", action="store_true")
    r.add_argument("args", nargs="*", help="run-args as key=value")

    s = sub.add_parser("status", help="show per-stage completion from markers")
    s.add_argument("spec")

    i = sub.add_parser("infra", help="provision/destroy AWS infra")
    i.add_argument("action", choices=["apply", "destroy"])
    i.add_argument("spec")

    w = sub.add_parser("worker", help="run one local poll-loop worker")
    w.add_argument("spec")

    v = sub.add_parser("viz", help="serve a snapshot json")
    v.add_argument("snapshot")
    v.add_argument("--port", type=int, default=8765)

    a = ap.parse_args(argv)

    if a.cmd == "run":
        pipe = _load(a.spec)
        if a.backend == "aws":
            from .infra.apply import load_outputs
            load_outputs(pipe)
        snap = pipe.run(backend=a.backend, workers=a.workers, viz=a.viz,
                        force=a.force, smoke=not a.no_smoke, **_kv(a.args))
        print(json.dumps({st["name"]: st["counts"] for st in snap["stages"]}, indent=2))
        return 0

    if a.cmd == "status":
        pipe = _load(a.spec)
        from .store import store_from_config
        store = store_from_config(pipe.config)
        for st in pipe.topo_order():
            done = len(store.list(f"results/{st.name}/"))
            print(f"{st.name:24s} {done} chunks done")
        return 0

    if a.cmd == "infra":
        pipe = _load(a.spec)
        from .infra import apply as infra
        if a.action == "apply":
            print(json.dumps(infra.apply(pipe), indent=2))
        else:
            infra.destroy(pipe)
        return 0

    if a.cmd == "worker":
        pipe = _load(a.spec)
        from .fleet.local import LocalFleet
        from .queue.local import LocalQueue
        from .store import store_from_config
        store = store_from_config(pipe.config)
        q = LocalQueue()
        from . import worker as worker_mod
        worker_mod.poll_loop(queue=q, store=store, pipe=pipe,
                             idle_timeout=pipe.config.worker_idle_timeout)
        return 0

    if a.cmd == "viz":
        from .web.server import serve_snapshot
        serve_snapshot(a.snapshot, port=a.port)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
