"""Local visualization webserver (stdlib only — no Flask/d3).

Serves a single-page dashboard that polls ``/api/state`` and draws the run: each
**stage is a group** (a card), each **task is a small square** colored by state
(queued / running / success / failed / ...), and SVG edges show how one group of
tasks feeds the next. Backed by the live ``Tracker``, so it works for both the local
and AWS backends. Also usable standalone against a static snapshot JSON file.
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_HERE = os.path.dirname(__file__)


def _index_html() -> bytes:
    with open(os.path.join(_HERE, "index.html"), "rb") as f:
        return f.read()


def start_server(tracker, *, host: str = "127.0.0.1", port: int = 8765):
    """Start the dashboard in a daemon thread. Returns the HTTPServer."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # silence access logs
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                self._send(200, _index_html(), "text/html; charset=utf-8")
            elif self.path.startswith("/api/state"):
                body = json.dumps(tracker.snapshot()).encode()
                self._send(200, body, "application/json")
            else:
                self._send(404, b"not found", "text/plain")

    httpd = ThreadingHTTPServer((host, port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
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
