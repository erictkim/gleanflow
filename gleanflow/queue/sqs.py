"""SQS-backed task plane for the AWS backend.

Maps the leased-queue semantics onto SQS: the visibility timeout *is* the lease,
``extend`` is ChangeMessageVisibility, ``ack`` is DeleteMessage, and ``fail`` makes
the message visible again immediately. Dead-lettering is handled by the queue's
redrive policy (``maxReceiveCount`` -> DLQ), provisioned by the infra layer.
"""

from __future__ import annotations

from typing import Optional

from ..task import Task
from .base import Queue


class SqsQueue(Queue):
    def __init__(self, queue_url: str, region: str = "us-east-1"):
        import boto3
        self.url = queue_url
        self.sqs = boto3.client("sqs", region_name=region)
        self._receipts: dict[str, str] = {}   # task.key -> receipt handle

    def enqueue(self, task: Task) -> None:
        self.sqs.send_message(QueueUrl=self.url, MessageBody=task.to_json())

    def claim(self, lease: float) -> Optional[Task]:
        r = self.sqs.receive_message(
            QueueUrl=self.url, MaxNumberOfMessages=1,
            WaitTimeSeconds=2, VisibilityTimeout=int(lease),
            AttributeNames=["ApproximateReceiveCount"],
        )
        msgs = r.get("Messages", [])
        if not msgs:
            return None
        m = msgs[0]
        task = Task.from_json(m["Body"])
        task.attempt = int(m.get("Attributes", {}).get("ApproximateReceiveCount", 1)) - 1
        self._receipts[task.key] = m["ReceiptHandle"]
        return task

    def extend(self, task: Task, lease: float) -> None:
        rh = self._receipts.get(task.key)
        if rh:
            self.sqs.change_message_visibility(
                QueueUrl=self.url, ReceiptHandle=rh, VisibilityTimeout=int(lease))

    def ack(self, task: Task) -> None:
        rh = self._receipts.pop(task.key, None)
        if rh:
            self.sqs.delete_message(QueueUrl=self.url, ReceiptHandle=rh)

    def fail(self, task: Task) -> None:
        rh = self._receipts.pop(task.key, None)
        if rh:
            # make visible again now; SQS redrive policy dead-letters after maxReceiveCount
            self.sqs.change_message_visibility(
                QueueUrl=self.url, ReceiptHandle=rh, VisibilityTimeout=0)

    def depth(self) -> int:
        a = self.sqs.get_queue_attributes(
            QueueUrl=self.url,
            AttributeNames=["ApproximateNumberOfMessages",
                            "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return int(a.get("ApproximateNumberOfMessages", 0)) + \
            int(a.get("ApproximateNumberOfMessagesNotVisible", 0))
