"""Webserver triage endpoints that spawn a Claude session (stubbed LLM)."""

import json
import urllib.error
import urllib.request

from gleanflow import Investigator, Pipeline, PipelineConfig
from gleanflow.agent import Investigator as Inv
from gleanflow.controller import StageFailed
from gleanflow.store import store_from_config
from gleanflow.tracker import Tracker
from gleanflow.web.server import start_server

STUB = lambda prompt: '{"action":"resplit","diagnosis":"oom on packed chunk","fix":"split it"}'


def _failed_run(tmp_path):
    pipe = Pipeline("web", PipelineConfig(s3_root=str(tmp_path), max_redeliveries=1))
    src = pipe.source("nums", chunks=[{"id": f"n{i}", "params": {"v": i}, "weight": 1.0}
                                      for i in range(2)])

    @pipe.stage(reads=src, name="gen", target_rows=2)
    def gen(ctx):
        raise MemoryError("boom")

    try:
        pipe.run(backend="local", smoke=False)
    except StageFailed:
        pass
    return pipe


def _post(port, path, body=None):
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method="POST")
    return json.load(urllib.request.urlopen(req))


def test_investigator_diagnose(tmp_path):
    pipe = _failed_run(tmp_path)
    store = store_from_config(pipe.config)
    failures = [k[len("failures/"):-len(".json")]
                for k in store.list("failures/gen/") if k.endswith(".json")]
    inv = Inv(store, pipe, complete=STUB)
    out = inv.diagnose(failures[0])
    assert out["action"] == "resplit" and "oom" in out["diagnosis"]


def test_post_diagnose_endpoint(tmp_path):
    pipe = _failed_run(tmp_path)
    store = store_from_config(pipe.config)
    key = next(k[len("failures/"):-len(".json")]
               for k in store.list("failures/gen/") if k.endswith(".json"))
    tr = Tracker("web"); tr.add_stage("gen", [])
    start_server(tr, store=store, pipe=pipe, investigator=Investigator(store, pipe, complete=STUB),
                 port=8793)
    out = _post(8793, "/api/diagnose?key=" + key)
    assert out["action"] == "resplit"

    health = _post(8793, "/api/check")
    assert "gen" in health["summary"]


def test_post_disabled_without_investigator(tmp_path):
    tr = Tracker("web2"); tr.add_stage("gen", [])
    start_server(tr, store=None, pipe=None, investigator=None, port=8794)
    try:
        _post(8794, "/api/diagnose?key=gen/c0")
        assert False, "expected 503"
    except urllib.error.HTTPError as e:
        assert e.code == 503
