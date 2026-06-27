"""Local visualization + query webserver (stdlib only — no Flask/d3).

Serves a single-page dashboard (each stage a group, each task a colored square, SVG
edges between groups) and a small JSON API a local LLM agent can curl to inspect the
run:

    GET /api/state                       full snapshot (stages, deps, task states)
    GET /api/failures                    every failed chunk + traceback + OOM stats
    GET /api/task?key=<stage/chunk>      one task: live state + result/failure markers
    GET /api/stage?name=<stage>          stage counts, deps, and source code

Local only (binds 127.0.0.1, no auth). Backed by the live ``Tracker`` plus the run's
object store, so it works for both the local and AWS backends.
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

_HERE = os.path.dirname(__file__)


def _index_html() -> bytes:
    with open(os.path.join(_HERE, "index.html"), "rb") as f:
        return f.read()


def start_server(tracker, *, store=None, pipe=None, host: str = "127.0.0.1", port: int = 8765):
    """Start the dashboard + query API in a daemon thread. Returns the HTTPServer."""

    def _failures() -> list:
        from .. import markers
        out = []
        if store is None:
            return out
        snap = tracker.snapshot()
        for st in snap["stages"]:
            for key in markers.list_failures(store, st["name"]):
                try:
                    out.append(markers.read_failure(store, key))
                except Exception:
                    pass
        return out

    def _task(key: str) -> dict:
        from .. import markers
        d: dict = {"key": key}
        if store is not None:
            if markers.has_result(store, key):
                d["result"] = markers.read_result(store, key)
            if markers.has_failure(store, key):
                d["failure"] = markers.read_failure(store, key)
        for st in tracker.snapshot()["stages"]:
            for t in st["tasks"]:
                if t["key"] == key:
                    d["state"] = t.get("state")
        return d

    def _stage(name: str) -> dict:
        import inspect
        d: dict = {"name": name}
        for st in tracker.snapshot()["stages"]:
            if st["name"] == name:
                d["counts"], d["deps"] = st["counts"], st["deps"]
        if pipe is not None and name in pipe.stages:
            try:
                d["source"] = inspect.getsource(pipe.stages[name].fn)
            except Exception:
                d["source"] = ""
        return d

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj):
            self._send(200, json.dumps(obj).encode(), "application/json")

        def do_GET(self):
            u = urlparse(self.path)
            q = parse_qs(u.query)
            if u.path in ("/", "/index.html"):
                self._send(200, _index_html(), "text/html; charset=utf-8")
            elif u.path == "/api/state":
                self._json(tracker.snapshot())
            elif u.path == "/api/failures":
                self._json(_failures())
            elif u.path == "/api/task":
                self._json(_task(q.get("key", [""])[0]))
            elif u.path == "/api/stage":
                self._json(_stage(q.get("name", [""])[0]))
            else:
                self._send(404, b"not found", "text/plain")

    httpd = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def serve_snapshot(path: str, *, host="127.0.0.1", port=8765):
    """Serve a static snapshot JSON file (CLI ``gleanflow viz <file>``)."""
    class _Static:
        def snapshot(self):
            with open(path) as f:
                return json.load(f)

    httpd = start_server(_Static(), host=host, port=port)
    print(f"[viz] http://{host}:{port}  (snapshot {path})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
