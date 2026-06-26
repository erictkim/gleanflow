"""Unit tests for the task plane (lease/redelivery/DLQ) and the chunk packer."""

import time

from gleanflow.partition import Chunk, pack
from gleanflow.queue.local import LocalQueue
from gleanflow.task import Task


def test_lease_redelivery_then_dlq():
    q = LocalQueue(max_redeliveries=2)
    t = Task(stage="s", key="s/c0", params={})
    q.enqueue(t)

    t1 = q.claim(lease=0.01)
    assert t1.key == "s/c0" and t1.attempt == 0

    time.sleep(0.03)                      # lease expires
    t2 = q.claim(lease=0.01)
    assert t2.key == "s/c0" and t2.attempt == 1   # redelivered

    time.sleep(0.03)
    t3 = q.claim(lease=0.01)
    assert t3.attempt == 2

    time.sleep(0.03)
    assert q.claim(lease=0.01) is None    # exhausted -> dead-lettered
    assert len(q.dlq) == 1 and q.dlq[0].key == "s/c0"


def test_ack_removes_inflight():
    q = LocalQueue()
    t = Task(stage="s", key="s/c1", params={})
    q.enqueue(t)
    claimed = q.claim(lease=10)
    q.ack(claimed)
    assert q.depth() == 0
    time.sleep(0.01)
    assert q.claim(lease=1) is None       # acked task never comes back


def test_fail_redelivers_immediately():
    q = LocalQueue(max_redeliveries=3)
    t = Task(stage="s", key="s/c2", params={})
    q.enqueue(t)
    c = q.claim(lease=100)
    q.fail(c)                             # negative ack
    again = q.claim(lease=100)
    assert again.key == "s/c2" and again.attempt == 1


def test_packer_balances_and_preserves_members():
    members = [Chunk(id=f"m{i}", params={"i": i}, weight=10.0) for i in range(3)]
    members.append(Chunk(id="m3", params={"i": 3}, weight=5.0))
    chunks = pack(members, target=20.0)

    assert len(chunks) == 2
    assert chunks[0].weight == 20.0 and chunks[1].weight == 15.0
    total_members = sum(len(c.params["_members"]) for c in chunks)
    assert total_members == 4
    assert chunks[0].params["_members"][0]["id"] == "m0"
