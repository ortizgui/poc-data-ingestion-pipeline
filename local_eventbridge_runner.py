from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import boto3
from botocore.exceptions import ClientError

from aws_local import AWS_ENDPOINT_URL, AWS_REGION, EVENTBRIDGE_QUEUE, PipelineError
from analytics_queries import print_analytics_report, run_analytics_queries
from evidence import evidence_table
from local_sfn_runner import run_state_machine


def aws_client(service: str) -> Any:
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def queue_url(sqs: Any, queue_name: str) -> str:
    return sqs.create_queue(QueueName=queue_name)["QueueUrl"]


def event_detail(body: dict[str, Any]) -> dict[str, Any] | None:
    if "detail" in body:
        detail = body["detail"]
        return json.loads(detail) if isinstance(detail, str) else detail
    if "Detail" in body:
        detail = body["Detail"]
        return json.loads(detail) if isinstance(detail, str) else detail
    if {"product", "file_name", "business_date"}.issubset(body):
        return body
    return None


def run_eventbridge_runner(queue_name: str = EVENTBRIDGE_QUEUE, max_messages: int = 10) -> dict[str, Any]:
    sqs = aws_client("sqs")
    queue = queue_url(sqs, queue_name)
    processed = []
    skipped = []
    received = 0

    while received < max_messages:
        response = sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        messages = response.get("Messages", [])
        if not messages:
            break
        for message in messages:
            received += 1
            body = json.loads(message["Body"])
            detail = event_detail(body)
            try:
                if not detail:
                    raise PipelineError("message is not EventBridge file-ready event")
                processed.append(run_state_machine(detail))
            except PipelineError as exc:
                try:
                    report = json.loads(str(exc))
                    skipped.append({"reason": report["steps"][-1]["error"], "evidence": report})
                except (json.JSONDecodeError, KeyError):
                    skipped.append({"reason": str(exc)})
            finally:
                sqs.delete_message(QueueUrl=queue, ReceiptHandle=message["ReceiptHandle"])

    return {"received_messages": received, "processed": processed, "skipped": skipped}


def print_summary(result: dict[str, Any]) -> None:
    print(f"EventBridge runner summary: processed={len(result['processed'])} skipped={len(result['skipped'])}")
    print("")
    for item in result["processed"]:
        print(evidence_table(item["evidence"]))
        print("")
    for item in result["skipped"]:
        if "evidence" in item:
            print(evidence_table(item["evidence"]))
        else:
            print(f"SKIP: {item['reason']}")
        print("")
    try:
        print_analytics_report(run_analytics_queries())
    except Exception as exc:
        print(f"\nAnalytics query simulation skipped: {exc}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll local EventBridge SQS target and run local Step Functions")
    parser.add_argument("--queue", default=EVENTBRIDGE_QUEUE)
    parser.add_argument("--max-messages", type=int, default=10)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        result = run_eventbridge_runner(args.queue, args.max_messages)
    except ClientError as exc:
        print(f"eventbridge runner failed: {exc}", file=sys.stderr)
        return 1
    print_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
