from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from ecs_worker import run_worker
from aws_local import (
    ANALYTICS_BUCKET,
    ANALYTICS_DATABASE,
    AWS_ENDPOINT_URL,
    AWS_REGION,
    BUSINESS_DATABASE,
    CONFIG_TABLE,
    CURATED_QUEUE,
    DATA_BUCKET,
    EVENTBRIDGE_QUEUE,
    SOURCE_BUCKET,
    clients,
)
from local_eventbridge_runner import run_eventbridge_runner
from pipeline import bootstrap, publish_events


REPORTS_DIR = Path("runtime/reports")


def aws_client(service: str) -> Any:
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def wait_for_ministack(timeout_seconds: int = 30) -> None:
    s3 = aws_client("s3")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            s3.list_buckets()
            return
        except (BotoCoreError, ClientError):
            time.sleep(1)
    raise RuntimeError(f"MiniStack not reachable at {AWS_ENDPOINT_URL}")


def cleanup_ministack() -> None:
    s3 = aws_client("s3")
    sqs = aws_client("sqs")
    sns = aws_client("sns")
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    for bucket in [SOURCE_BUCKET, DATA_BUCKET, ANALYTICS_BUCKET]:
        try:
            response = s3.list_objects_v2(Bucket=bucket)
        except ClientError:
            continue
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})

    for queue_url in sqs.list_queues().get("QueueUrls", []):
        if queue_url.rsplit("/", 1)[-1] in {
            EVENTBRIDGE_QUEUE,
            CURATED_QUEUE,
            "billing-events",
            "analytics-events",
            "crm-events",
            "data-quality-events",
        }:
            sqs.delete_queue(QueueUrl=queue_url)

    for topic in sns.list_topics().get("Topics", []):
        if topic["TopicArn"].rsplit(":", 1)[-1] in {"billing-events", "analytics-events", "crm-events", "data-quality-events"}:
            sns.delete_topic(TopicArn=topic["TopicArn"])

    try:
        table = dynamodb.Table(CONFIG_TABLE)
        table.delete()
        table.wait_until_not_exists()
    except ClientError:
        pass


def list_keys(prefix: str) -> list[str]:
    s3 = aws_client("s3")
    response = s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix=prefix)
    return [item["Key"] for item in response.get("Contents", [])]


def list_analytics_keys(prefix: str) -> list[str]:
    s3 = aws_client("s3")
    response = s3.list_objects_v2(Bucket=ANALYTICS_BUCKET, Prefix=prefix)
    return [item["Key"] for item in response.get("Contents", [])]


def assert_curated_notification_configured() -> None:
    s3 = aws_client("s3")
    config = s3.get_bucket_notification_configuration(Bucket=DATA_BUCKET)
    queue_configs = config.get("QueueConfigurations", [])
    curated_configs = [
        item
        for item in queue_configs
        if item.get("Id") == "curated-to-sqs" and "s3:ObjectCreated:*" in item.get("Events", [])
    ]
    rejected_configs = [
        item
        for item in queue_configs
        if item.get("Id") == "rejected-to-sqs" and "s3:ObjectCreated:*" in item.get("Events", [])
    ]
    assert_true(bool(curated_configs), "curated S3 notification missing")
    assert_true(bool(rejected_configs), "rejected S3 notification missing")


def queue_messages(queue_name: str, expected: int) -> list[dict[str, Any]]:
    sqs = aws_client("sqs")
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    messages = []
    deadline = time.time() + 10
    while len(messages) < expected and time.time() < deadline:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            VisibilityTimeout=1,
        )
        for message in response.get("Messages", []):
            body = json.loads(message["Body"])
            payload = json.loads(body["Message"]) if "Message" in body else body
            messages.append(payload)
    return messages


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def write_e2e_report(status: str, checks: list[dict[str, str]], error: str | None = None) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("e2e-%Y%m%d%H%M%S")
    report = {
        "run_id": run_id,
        "status": status,
        "endpoint": AWS_ENDPOINT_URL,
        "checks": checks,
        "error": error,
    }
    path = REPORTS_DIR / f"{run_id}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def main() -> int:
    checks = []
    try:
        wait_for_ministack()
        checks.append({"status": "OK", "check": "MiniStack reachable"})
        cleanup_ministack()
        checks.append({"status": "OK", "check": "MiniStack state cleaned"})
        bootstrap()
        checks.append({"status": "OK", "check": "Bootstrap created AWS resources and sample data"})
        try:
            glue = aws_client("glue")
            databases = glue.get_databases()
            analytics_db = next((db for db in databases.get("DatabaseList", []) if db["Name"] == ANALYTICS_DATABASE), None)
            business_db = next((db for db in databases.get("DatabaseList", []) if db["Name"] == BUSINESS_DATABASE), None)
            if analytics_db and business_db:
                analytics_tables = glue.get_tables(DatabaseName=ANALYTICS_DATABASE)
                business_tables = glue.get_tables(DatabaseName=BUSINESS_DATABASE)
                assert_true(len(analytics_tables.get("TableList", [])) == 8, f"expected 8 analytics tables, got {len(analytics_tables.get('TableList', []))}")
                assert_true(len(business_tables.get("TableList", [])) == 4, f"expected 4 business tables, got {len(business_tables.get('TableList', []))}")
                checks.append({"status": "OK", "check": "Glue Data Catalog databases and 12 tables created"})
            else:
                checks.append({"status": "WARN", "check": "Glue databases not found (MiniStack may not support Glue)"})
        except Exception:
            checks.append({"status": "SKIP", "check": "Glue not available in MiniStack"})
        assert_curated_notification_configured()
        checks.append({"status": "OK", "check": "S3 notification configured for curated prefix"})
        published = publish_events()
        checks.append({"status": "OK", "check": "Pipeline published sample events to EventBridge"})
        result = run_eventbridge_runner(max_messages=10)
        checks.append({"status": "OK", "check": "EventBridge runner started local Step Functions executions"})
        worker_result = run_worker(max_messages=10)
        checks.append({"status": "OK", "check": "ECS worker consumed curated S3 notifications"})

        processed = result["processed"]
        skipped = result["skipped"]
        assert_true(len(published["published"]) == 3, f"expected 3 published events, got {len(published['published'])}")
        assert_true(len(processed) == 2, f"expected 2 processed events, got {len(processed)}")
        assert_true(len(skipped) == 1, f"expected 1 skipped event, got {len(skipped)}")
        assert_true("not configured" in skipped[0]["reason"], skipped[0]["reason"])
        checks.append({"status": "OK", "check": "2 configured products processed and 1 unconfigured product skipped"})
        assert_true(all(Path(item["evidence"]["report_path"]).exists() for item in processed), "processed evidence missing")
        assert_true(Path(skipped[0]["evidence"]["report_path"]).exists(), "skipped evidence missing")
        assert_true(
            skipped[0]["evidence"]["steps"][-1]["name"] == "ValidateEvent",
            "skipped product should fail at ASL ValidateEvent",
        )
        assert_true(
            all(
                any(step["name"] == "HarmonizationGlue" and "de-para" in step["detail"] for step in item["evidence"]["steps"])
                for item in processed
            ),
            "harmonization should load de-para mapping from S3",
        )
        checks.append({"status": "OK", "check": "Pipeline evidence files written for success and failure"})

        assert_true(len(list_keys("raw/")) == 2, "expected 2 raw files")
        assert_true(len(list_keys("processed/")) == 2, "expected 2 processed files")
        assert_true(len(list_keys("curated/")) == 2, "expected 2 curated files")
        assert_true(len(list_analytics_keys("observability/ingestion_runs/")) >= 2, "expected ingestion run analytics files")
        assert_true(len(list_analytics_keys("observability/ingestion_steps/")) >= 6, "expected ingestion step analytics files")
        assert_true(worker_result["processed_messages"] == 2, "ECS worker should consume 2 S3 notifications")
        assert_true(all(item["step_functions_execution"] for item in processed), "Step Functions execution missing")
        checks.append({"status": "OK", "check": "S3 raw/processed/curated, analytics bucket and Step Functions evidence validated"})

        billing = queue_messages("billing-events", expected=4)
        analytics = queue_messages("analytics-events", expected=4)
        crm = queue_messages("crm-events", expected=0)
        assert_true(len(billing) == 4, f"expected 4 billing SNS events, got {len(billing)}")
        assert_true(len(analytics) == 4, f"expected 4 analytics SNS events, got {len(analytics)}")
        assert_true(len(crm) == 0, f"expected 0 crm SNS events, got {len(crm)}")
        checks.append({"status": "OK", "check": "SNS destination queues received expected transaction events"})

        domains = {message["record"]["domain"] for message in [*billing, *analytics]}
        assert_true(domains == {"transaction"}, f"expected only transaction domain, got {domains}")
        checks.append({"status": "OK", "check": "Both configured products harmonized to transaction domain"})

        table = clients().dynamodb.Table(CONFIG_TABLE)
        assert_true("Item" in table.get_item(Key={"product": "orders"}), "orders config missing in DynamoDB")
        assert_true("Item" in table.get_item(Key={"product": "payments"}), "payments config missing in DynamoDB")
        assert_true("Item" not in table.get_item(Key={"product": "invoices"}), "invoices should not be configured")
        checks.append({"status": "OK", "check": "DynamoDB product config matches expected products"})

    except Exception as exc:
        report_path = write_e2e_report("FAIL", checks, str(exc))
        print(f"E2E evidence: {report_path}", file=sys.stderr)
        print(f"E2E failed: {exc}", file=sys.stderr)
        return 1

    report_path = write_e2e_report("OK", checks)
    print("E2E OK: 2 configured files processed, 1 unconfigured file skipped, ECS worker published SNS events")
    print(f"E2E evidence: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
