from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError

from aws_local import CONFIG_TABLE, CURATED_QUEUE, normalize_rejection_policy, service_client, service_resource


def get_queue_url(sqs: Any, queue_name: str) -> str:
    return sqs.create_queue(QueueName=queue_name)["QueueUrl"]


def ensure_topic_and_destination_queue(sns: Any, sqs: Any, destination: str) -> tuple[str, str]:
    topic_arn = sns.create_topic(Name=destination)["TopicArn"]
    queue_url = get_queue_url(sqs, destination)
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
    if not any(item.get("Endpoint") == queue_arn for item in existing):
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    return topic_arn, queue_url


def read_jsonl_rows(s3: Any, bucket: str, key: str) -> list[dict[str, Any]]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def product_from_file_key(key: str) -> str:
    parts = key.split("/")
    if len(parts) >= 2 and parts[0] == "curated":
        return parts[1]
    if len(parts) >= 3 and parts[0] == "rejected":
        return parts[2]
    raise ValueError(f"unexpected pipeline file key: {key}")


def run_id_from_file_key(key: str) -> str:
    parts = key.split("/")
    if parts and parts[0] == "curated":
        return parts[5] if len(parts) > 5 else "unknown"
    if parts and parts[0] == "rejected":
        return parts[6] if len(parts) > 6 else "unknown"
    return "unknown"


def product_config(product: str) -> dict[str, Any]:
    response = service_resource("dynamodb").Table(CONFIG_TABLE).get_item(Key={"product": product})
    if "Item" not in response:
        raise ValueError(f"product not configured in DynamoDB: {product}")
    return response["Item"]


def s3_records(message: dict[str, Any]) -> list[dict[str, str]]:
    if message.get("Event") == "s3:TestEvent":
        return []

    records = []
    for record in message.get("Records", []):
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        if bucket and key:
            records.append({"bucket": bucket, "key": unquote_plus(key)})
    if records:
        return records

    if {"bucket", "key"}.issubset(message):
        return [{"bucket": message["bucket"], "key": message["key"]}]

    raise ValueError("message is not S3 notification")


def publish_curated_file(record: dict[str, str], sns: Any, sqs: Any, s3: Any) -> list[str]:
    product = product_from_file_key(record["key"])
    config = product_config(product)
    rows = read_jsonl_rows(s3, record["bucket"], record["key"])
    published = []
    for destination in config["publish"]["destinations"]:
        topic_arn, _queue_url = ensure_topic_and_destination_queue(sns, sqs, destination)
        for row in rows:
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(
                    {
                        "event_type": "domain_record_ready",
                        "destination": destination,
                        "run_id": run_id_from_file_key(record["key"]),
                        "product": product,
                        "record": row,
                        "source": {"bucket": record["bucket"], "key": record["key"]},
                    },
                    sort_keys=True,
                ),
            )
        published.append(destination)
    return published


def publish_rejected_file(message: dict[str, Any], record: dict[str, str], sns: Any, sqs: Any, s3: Any) -> list[str]:
    product = message.get("product") or product_from_file_key(record["key"])
    config = product_config(product)
    policy = normalize_rejection_policy(product, config)
    event_type = message.get("event_type") or "ingestion.records-rejected"
    rows = read_jsonl_rows(s3, record["bucket"], record["key"]) if event_type == "ingestion.records-rejected" else []
    published = []
    for destination in policy["destinations"]:
        topic_arn, _queue_url = ensure_topic_and_destination_queue(sns, sqs, destination)
        if event_type == "ingestion.file-failed":
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps(
                    {
                        "event_type": "ingestion.file-failed",
                        "destination": destination,
                        "run_id": message.get("run_id") or run_id_from_file_key(record["key"]),
                        "product": product,
                        "business_date": message.get("business_date"),
                        "file_name": message.get("file_name"),
                        "rows": message.get("rows", {}),
                        "error": message.get("error"),
                        "source": {"bucket": record["bucket"], "key": record["key"]},
                    },
                    sort_keys=True,
                ),
            )
        else:
            for row in rows:
                sns.publish(
                    TopicArn=topic_arn,
                    Message=json.dumps(
                        {
                            "event_type": "ingestion.records-rejected",
                            "destination": destination,
                            "run_id": message.get("run_id") or run_id_from_file_key(record["key"]),
                            "product": product,
                            "rejected_record": row,
                            "source": {"bucket": record["bucket"], "key": record["key"]},
                        },
                        sort_keys=True,
                    ),
                )
        published.append(destination)
    return published


def process_message(message: dict[str, Any], sns: Any, sqs: Any, s3: Any) -> list[str]:
    published = []
    for record in s3_records(message):
        if record["key"].startswith("rejected/") or message.get("event_type") in {
            "ingestion.records-rejected",
            "ingestion.file-failed",
        }:
            published.extend(publish_rejected_file(message, record, sns, sqs, s3))
        else:
            published.extend(publish_curated_file(record, sns, sqs, s3))
    return published


def run_worker(queue_name: str = CURATED_QUEUE, max_messages: int = 10) -> dict[str, Any]:
    sqs = service_client("sqs")
    sns = service_client("sns")
    s3 = service_client("s3")
    queue_url = get_queue_url(sqs, queue_name)
    processed = 0
    received = 0
    destinations: list[str] = []

    while received < max_messages:
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        messages = response.get("Messages", [])
        if not messages:
            break
        for sqs_message in messages:
            body = json.loads(sqs_message["Body"])
            published = process_message(body, sns, sqs, s3)
            destinations.extend(published)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=sqs_message["ReceiptHandle"])
            received += 1
            if published:
                processed += 1

    return {"processed_messages": processed, "received_messages": received, "published_destinations": sorted(set(destinations))}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local ECS worker: consume curated SQS and publish SNS")
    parser.add_argument("--queue", default=os.getenv("CURATED_QUEUE", CURATED_QUEUE))
    parser.add_argument("--max-messages", type=int, default=10)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        result = run_worker(args.queue, args.max_messages)
    except ClientError as exc:
        print(f"ecs worker failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
