from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from aws_local import (
    ANALYTICS_BUCKET,
    CONFIG_TABLE,
    CURATED_QUEUE,
    DATA_BUCKET,
    DEFAULT_CONFIG,
    DEFAULT_EVENTS,
    DEFAULT_FILES,
    DEFAULT_MAPPINGS,
    EVENT_BUS,
    EVENTBRIDGE_QUEUE,
    PipelineError,
    SOURCE_BUCKET,
    STATE_MACHINE_NAME,
    clients,
    ensure_bucket,
    ensure_queue,
    ensure_table,
    load_json,
    queue_arn,
)


ASL_PATH = Path(__file__).resolve().parent / "state-machine.asl.json"


def run_id_for(event: dict[str, Any]) -> str:
    raw = json.dumps(event, sort_keys=True).encode("utf-8")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{stamp}-{hashlib.sha1(raw).hexdigest()[:8]}"


def configure_curated_notification(aws: Any) -> str:
    queue_url = ensure_queue(aws.sqs, CURATED_QUEUE)
    aws.s3.put_bucket_notification_configuration(
        Bucket=DATA_BUCKET,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "Id": "curated-to-sqs",
                    "QueueArn": queue_arn(aws.sqs, queue_url),
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": "curated/"},
                                {"Name": "suffix", "Value": ".parquet"},
                            ]
                        }
                    },
                },
                {
                    "Id": "rejected-to-sqs",
                    "QueueArn": queue_arn(aws.sqs, queue_url),
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": "rejected/"},
                                {"Name": "suffix", "Value": ".jsonl"},
                            ]
                        }
                    },
                },
            ]
        },
    )
    return queue_url


def configure_eventbridge_target(aws: Any) -> str:
    queue_url = ensure_queue(aws.sqs, EVENTBRIDGE_QUEUE)
    try:
        aws.events.create_event_bus(Name=EVENT_BUS)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise

    aws.events.put_rule(
        Name="file-ready-to-local-runner",
        EventPattern=json.dumps({"source": ["local.product-lake"], "detail-type": ["file-ready"]}),
        EventBusName=EVENT_BUS,
        State="ENABLED",
    )
    aws.events.put_targets(
        Rule="file-ready-to-local-runner",
        EventBusName=EVENT_BUS,
        Targets=[{"Id": "eventbridge-file-ready-queue", "Arn": queue_arn(aws.sqs, queue_url)}],
    )
    return queue_url


def seed_product_configs(config_path: Path = DEFAULT_CONFIG, aws: Any | None = None) -> None:
    aws = aws or clients()
    ensure_table(aws.dynamodb)
    table = aws.dynamodb.Table(CONFIG_TABLE)
    config = load_json(config_path)
    for product, product_config in config["products"].items():
        table.put_item(Item={"product": product, **product_config})


def upload_mapping_files(mappings_dir: Path = DEFAULT_MAPPINGS, aws: Any | None = None) -> None:
    aws = aws or clients()
    ensure_bucket(aws.s3, DATA_BUCKET)
    for path in mappings_dir.glob("*.json"):
        aws.s3.upload_file(str(path), DATA_BUCKET, f"de-para/{path.name}")


def upload_sample_files(source_dir: Path = DEFAULT_FILES, aws: Any | None = None) -> None:
    aws = aws or clients()
    ensure_bucket(aws.s3, SOURCE_BUCKET)
    for path in source_dir.rglob("*.csv"):
        key = path.relative_to(source_dir).as_posix()
        aws.s3.upload_file(str(path), SOURCE_BUCKET, key)


def ensure_state_machine(aws: Any) -> None:
    definition = ASL_PATH.read_text(encoding="utf-8")
    try:
        aws.sfn.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/local-step-functions",
            type="STANDARD",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "StateMachineAlreadyExists":
            raise


def bootstrap(config_path: Path = DEFAULT_CONFIG, source_dir: Path = DEFAULT_FILES) -> None:
    aws = clients()
    ensure_bucket(aws.s3, SOURCE_BUCKET)
    ensure_bucket(aws.s3, DATA_BUCKET)
    ensure_bucket(aws.s3, ANALYTICS_BUCKET)
    configure_curated_notification(aws)
    configure_eventbridge_target(aws)
    seed_product_configs(config_path, aws)
    upload_mapping_files(aws=aws)
    upload_sample_files(source_dir, aws)
    ensure_state_machine(aws)


def publish_event(event: dict[str, Any], aws: Any | None = None) -> dict[str, Any]:
    aws = aws or clients()
    run_id = event.get("run_id") or run_id_for(event)
    event = {**event, "run_id": run_id}
    response = aws.events.put_events(
        Entries=[
            {
                "Source": "local.product-lake",
                "DetailType": "file-ready",
                "EventBusName": EVENT_BUS,
                "Detail": json.dumps(event),
            }
        ]
    )
    return {
        "run_id": run_id,
        "product": event.get("product"),
        "file_name": event.get("file_name"),
        "business_date": event.get("business_date"),
        "eventbridge": f"event-bus://{EVENT_BUS}",
        "eventbridge_result": response.get("Entries", []),
    }


def publish_events(events_dir: Path = DEFAULT_EVENTS) -> dict[str, Any]:
    return {"published": [publish_event(load_json(event_path)) for event_path in sorted(events_dir.glob("*.json"))]}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish file-ready events to MiniStack EventBridge")
    parser.add_argument("event", nargs="?", type=Path, help="Event JSON. Omit to publish samples/events/*.json")
    parser.add_argument("--bootstrap", action="store_true", help="Create local AWS resources and upload config/sample files")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.bootstrap:
            bootstrap()
            if not args.event:
                print(json.dumps({"bootstrapped": True}, indent=2, sort_keys=True))
                return 0
        result = publish_event(load_json(args.event)) if args.event else publish_events()
    except (PipelineError, ClientError) as exc:
        print(f"pipeline failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
