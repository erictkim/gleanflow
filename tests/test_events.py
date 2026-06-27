"""Push notifications: in-process on_event callback + webhook registration."""

import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from gleanflow import Pipeline, PipelineConfig
from gleanflow.tracker import Tracker
from gleanflow.web.server import start_server


def _pipe(tmp_path):
    pipe = Pipeline("ev", PipelineConfig(s3_root=str(tmp_path)))
    src = pipe.source("s", chunks=[{"id": f"n{i}", "params": {"v": i}, "weight": 1.0}
                                   for i in range(3)])

    @pipe.stage(reads=src, name="gen")
    def gen(ctx):
        ctx.write([{"v": ctx.members[0]["v"]}])

    return pipe


def test_in_process_on_event(tmp_path):
    events = []
    _pipe(tmp_path).run(backend="local", on_event=events.append)

    types = {e["type"] for e in events}
    assert {"task", "stage_done", "run_done"} <= types
    assert any(e["type"] == "task" and e["state"] == "success" for e in events)
    # run_done arrives last
    assert events[-1]["type"] == "run_done"


def test_webhook_push(tmp_path):
    received = []

    class Recv(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            received.append(json.loads(self.rfile.read(n)))
            self.send_response(200)
            self.end_headers()

    recv = ThreadingHTTPServer(("127.0.0.1", 8796), Recv)
    threading.Thread(target=recv.serve_forever, daemon=True).start()

    tr = Tracker("ev")
    tr.add_stage("s", [])
    start_server(tr, port=8797)

    # Claude Code (or anything) registers a callback URL, filtered to terminal states
    req = urllib.request.Request(
        "http://127.0.0.1:8797/api/subscribe",
        data=json.dumps({"url": "http://127.0.0.1:8796/hook",
                         "events": ["failed", "success"]}).encode(), method="POST")
    sub = json.load(urllib.request.urlopen(req))
    assert "id" in sub

    tr.set_state("s", "s/c0", "running")    # filtered out
    tr.set_state("s", "s/c0", "success")    # pushed

    for _ in range(60):
        if received:
            break
        time.sleep(0.05)
    assert received, "webhook was not called"
    assert received[0]["state"] == "success" and received[0]["stage"] == "s"
