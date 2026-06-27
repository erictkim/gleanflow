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
import queue as _queue
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

_HERE = os.path.dirname(__file__)


def _index_html() -> bytes:
    with open(os.path.join(_HERE, "index.html"), "rb") as f:
        return f.read()


def start_server(tracker, *, store=None, pipe=None, investigator=None,
                 host: str = "127.0.0.1", port: int = 8765):
    """Start the dashboard + query API in a daemon thread. Returns the HTTPServer.

    If ``investigator`` is provided, POST endpoints spawn a Claude Code session:
        POST /api/diagnose?key=<stage/chunk>   -> structured remediation
        POST /api/ask        {question, key}   -> free-form answer
        POST /api/check                        -> run health verdict
    """

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

    # ---- push: webhooks + SSE, fed by a dispatcher subscribed to the tracker ---
    webhooks: dict = {}            # id -> {"url", "events": set|None}
    sse_queues: list = []          # [(Queue, filter_set|None)]
    _ids = [0]
    _evq: "_queue.Queue" = _queue.Queue()

    def _match(ev, flt):
        return flt is None or bool({ev.get("state"), ev.get("type")} & flt)

    def _dispatch():
        while True:
            ev = _evq.get()
            for wid, wh in list(webhooks.items()):
                if not _match(ev, wh["events"]):
                    continue
                try:
                    req = urllib.request.Request(
                        wh["url"], data=json.dumps(ev).encode(),
                        headers={"Content-Type": "application/json"}, method="POST")
                    urllib.request.urlopen(req, timeout=5)
                except Exception:
                    pass
            for q, flt in list(sse_queues):
                if _match(ev, flt):
                    try:
                        q.put_nowait(ev)
                    except Exception:
                        pass

    if hasattr(tracker, "subscribe"):
        tracker.subscribe(lambda ev: _evq.put(ev))
        threading.Thread(target=_dispatch, daemon=True).start()

    def _subscribe(body):
        wid = str(_ids[0]); _ids[0] += 1
        evs = body.get("events")
        webhooks[wid] = {"url": body["url"], "events": set(evs) if evs else None}
        return {"id": wid, "url": body["url"], "events": evs}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _sse(self, flt):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            q = _queue.Queue()
            sse_queues.append((q, flt))
            try:
                self.wfile.write(b": connected\n\n")
                self.wfile.flush()
                while True:
                    try:
                        ev = q.get(timeout=15)
                        self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())
                    except _queue.Empty:
                        self.wfile.write(b": keepalive\n\n")   # also detects disconnect
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                sse_queues.remove((q, flt))

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
            elif u.path == "/api/events":                      # SSE push stream
                flt = set(q.get("events", [""])[0].split(",")) - {""}
                self._sse(flt or None)
            elif u.path == "/api/subscriptions":
                self._json([{"id": k, "url": v["url"],
                             "events": list(v["events"]) if v["events"] else None}
                            for k, v in webhooks.items()])
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            u = urlparse(self.path)
            q = parse_qs(u.query)
            n = int(self.headers.get("Content-Length", 0) or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
            # --- notification subscriptions (no LLM needed) ---
            if u.path == "/api/subscribe":
                self._json(_subscribe(body))
                return
            if u.path == "/api/unsubscribe":
                webhooks.pop(body.get("id", ""), None)
                self._json({"ok": True})
                return
            # --- LLM triage (opt-in) ---
            if investigator is None:
                self._send(503, b'{"error":"agent api disabled (enable_agent_api)"}',
                           "application/json")
                return
            try:
                if u.path == "/api/diagnose":
                    self._json(investigator.diagnose(q.get("key", [body.get("key", "")])[0]))
                elif u.path == "/api/ask":
                    self._json(investigator.ask(body.get("question", ""), body.get("key")))
                elif u.path == "/api/check":
                    self._json(investigator.check_run())
                else:
                    self._send(404, b"not found", "text/plain")
            except Exception as e:  # never let a triage call crash the server
                self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")

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
