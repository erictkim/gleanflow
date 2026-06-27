"""LLM failure agent: failure markers, bounded auto-remediation, report policy."""

import json

import pytest

from gleanflow import (DiagnosticTools, FailureEvent, LLMFailureAgent, Pipeline,
                       PipelineConfig, Remediation)
from gleanflow.agent import _heuristic
from gleanflow.context import _read_parts
from gleanflow.controller import StageFailed
from gleanflow.store import store_from_config


def _records(store, prefix):
    out = []
    for k in store.list(prefix):
        if "/part-" in k:
            out.extend(_read_parts(store, k))
    return out


# ---- a pipeline whose producer OOMs on any multi-file (packed) chunk ----------
def _build(tmp_path, **cfg_kw):
    pipe = Pipeline("ag", PipelineConfig(s3_root=str(tmp_path), max_redeliveries=1,
                                         **cfg_kw))
    src = pipe.source("nums", chunks=[
        {"id": f"n{i}", "params": {"v": i + 1}, "weight": 1.0} for i in range(4)
    ])

    @pipe.stage(reads=src, name="gen", target_rows=2)   # packs 2 members per task
    def gen(ctx):
        members = ctx.members
        if len(members) > 1:
            raise MemoryError("simulated OOM on a packed chunk")
        ctx.write([{"v": members[0]["v"]}])

    @pipe.stage(reads=gen, name="total", target_rows=10 ** 9)
    def total(ctx):
        ctx.write([{"total": sum(r["v"] for r in ctx.input())}])

    return pipe


def test_failure_marker_is_written(tmp_path):
    pipe = _build(tmp_path)            # failure_policy default "off" -> no agent
    with pytest.raises(StageFailed):
        pipe.run(backend="local", smoke=False)

    store = store_from_config(pipe.config)
    failures = [k for k in store.list("failures/gen/") if k.endswith(".json")]
    assert failures
    body = json.loads(store.get_bytes(failures[0]))
    assert "MemoryError" in body["traceback"]
    assert body["error"]


def test_resplit_auto_recovers(tmp_path):
    # stub agent: always resplit the oversized chunk (no LLM call)
    pipe = _build(tmp_path, failure_policy="remediate", max_remediations=5,
                  failure_handler=lambda ev, tools: Remediation("resplit"))
    snap = pipe.run(backend="local", smoke=False)

    store = store_from_config(pipe.config)
    # the 2 packed chunks were split into 4 per-member sub-chunks, all succeeded
    gen_done = [k for k in store.list("results/gen/") if k.endswith(".json")]
    assert len(gen_done) == 4
    # downstream consumed the resplit outputs from the rewritten manifest
    assert _records(store, "data/total/")[0]["total"] == 1 + 2 + 3 + 4


def test_report_policy_does_not_mutate(tmp_path):
    # report policy: agent diagnoses but the run still fails (no auto-fix)
    seen = {}

    def handler(ev, tools):
        seen["diag"] = ev.error
        return Remediation("resplit", diagnosis="would split")   # ignored under report

    pipe = _build(tmp_path, failure_policy="report", failure_handler=handler)
    with pytest.raises(StageFailed):
        pipe.run(backend="local", smoke=False)
    assert "MemoryError" in seen["diag"]


def test_heuristic_oom_triage():
    multi = FailureEvent(stage="s", task_key="s/c0", params={}, attempt=1,
                         peak_mem_mb=15600, limit_mb=16384,
                         members=[{"id": "a"}, {"id": "b"}], resources={"mem": 16384})
    assert _heuristic(multi).action == "resplit"

    single = FailureEvent(stage="s", task_key="s/c1", params={}, attempt=1,
                          peak_mem_mb=15600, limit_mb=16384,
                          members=[{"id": "a"}], resources={"mem": 16384})
    r = _heuristic(single)
    assert r.action == "retry_with" and r.mem == 32768   # doubled, 4096-aligned


def test_llm_agent_parses_json(tmp_path):
    pipe = _build(tmp_path)
    tools = DiagnosticTools(store_from_config(pipe.config), pipe)
    agent = LLMFailureAgent(
        complete=lambda prompt: 'noise {"action":"skip","diagnosis":"d","fix":"f"} tail',
        notify=lambda m: None)
    ev = FailureEvent(stage="gen", task_key="gen/c0", params={}, attempt=1)
    rem = agent(ev, tools)
    assert rem.action == "skip" and rem.diagnosis == "d"
