from __future__ import annotations

import csv
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "products.json"
DEFAULT_MAPPINGS = ROOT / "config" / "mappings"
DEFAULT_EVENTS = ROOT / "samples" / "events"
DEFAULT_FILES = ROOT / "samples" / "product-lake"
DEFAULT_REPORTS = ROOT / "runtime" / "reports"

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "product-lake")
DATA_BUCKET = os.getenv("DATA_BUCKET", "data-lake")
ANALYTICS_BUCKET = os.getenv("ANALYTICS_BUCKET", "poc-data-ingestion-analytics")
CONFIG_TABLE = os.getenv("CONFIG_TABLE", "ProductConfig")
CURATED_QUEUE = os.getenv("CURATED_QUEUE", "curated-files")
EVENTBRIDGE_QUEUE = os.getenv("EVENTBRIDGE_QUEUE", "eventbridge-file-ready")
EVENT_BUS = os.getenv("EVENT_BUS", "ingestion-events")
STATE_MACHINE_NAME = os.getenv("STATE_MACHINE_NAME", "local-ingestion-state-machine")
DOMAIN_NAME = "transaction"
DOMAIN_REQUIRED_FIELDS = ("transaction_id", "customer_id", "amount", "transaction_date")
DEFAULT_REJECTION_POLICY = {
    "max_error_percent": 1,
    "max_error_count": 1000,
    "destinations": ["data-quality-events"],
}


class PipelineError(Exception):
    pass


class RejectedRowsThresholdError(PipelineError):
    def __init__(self, message: str, rejection_key: str, rejected_rows: int, total_rows: int) -> None:
        super().__init__(message)
        self.rejection_key = rejection_key
        self.rejected_rows = rejected_rows
        self.total_rows = total_rows


@dataclass(frozen=True)
class AwsClients:
    s3: Any
    dynamodb: Any
    sqs: Any
    events: Any
    sfn: Any


def clients() -> AwsClients:
    kwargs = {
        "endpoint_url": AWS_ENDPOINT_URL,
        "region_name": AWS_REGION,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return AwsClients(
        s3=boto3.client("s3", **kwargs),
        dynamodb=boto3.resource("dynamodb", **kwargs),
        sqs=boto3.client("sqs", **kwargs),
        events=boto3.client("events", **kwargs),
        sfn=boto3.client("stepfunctions", **kwargs),
    )


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_business_date(business_date: str) -> datetime:
    try:
        return datetime.strptime(business_date, "%Y-%m-%d")
    except ValueError as exc:
        raise PipelineError("business_date must use YYYY-MM-DD") from exc


def anomesdia_for(business_date: str) -> str:
    return parse_business_date(business_date).strftime("%Y%m%d")


def partition_prefix(business_date: str) -> str:
    date = parse_business_date(business_date)
    return f"year={date.year:04d}/month={date.month:02d}/day={date.day:02d}"


def ensure_bucket(s3: Any, bucket: str) -> None:
    try:
        s3.create_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def ensure_table(dynamodb: Any) -> None:
    existing = [table.name for table in dynamodb.tables.all()]
    if CONFIG_TABLE in existing:
        return
    table = dynamodb.create_table(
        TableName=CONFIG_TABLE,
        KeySchema=[{"AttributeName": "product", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "product", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()


def ensure_queue(sqs: Any, name: str) -> str:
    return sqs.create_queue(QueueName=name)["QueueUrl"]


def queue_arn(sqs: Any, queue_url: str) -> str:
    return sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def s3_read_text(s3: Any, bucket: str, key: str) -> str:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "NoSuchBucket"}:
            raise PipelineError(f"s3 object not found: s3://{bucket}/{key}") from exc
        raise
    return body.read().decode("utf-8")


def s3_write_text(s3: Any, bucket: str, key: str, text: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def read_csv_text(text: str) -> list[dict[str, str]]:
    return [dict(row) for row in csv.DictReader(io.StringIO(text))]


def write_jsonl(rows: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)


def validate_product_config(product: str, product_config: dict[str, Any]) -> None:
    if product_config.get("domain") != DOMAIN_NAME:
        raise PipelineError(f"product {product} must map to domain {DOMAIN_NAME}")
    if not product_config.get("mapping_key"):
        raise PipelineError(f"product {product} missing mapping_key")
    if "publish" not in product_config or not product_config["publish"].get("destinations"):
        raise PipelineError(f"product {product} missing publish destinations")
    normalize_rejection_policy(product, product_config)


def normalize_rejection_policy(product: str, product_config: dict[str, Any]) -> dict[str, Any]:
    policy = {**DEFAULT_REJECTION_POLICY, **product_config.get("rejection_policy", {})}
    max_error_percent = float(policy["max_error_percent"])
    max_error_count = int(policy["max_error_count"])
    destinations = policy.get("destinations", [])
    if max_error_percent < 0 or max_error_percent > 100:
        raise PipelineError(f"product {product} rejection_policy.max_error_percent must be between 0 and 100")
    if max_error_count < 0:
        raise PipelineError(f"product {product} rejection_policy.max_error_count must be >= 0")
    if not destinations:
        raise PipelineError(f"product {product} rejection_policy missing destinations")
    return {
        "max_error_percent": max_error_percent,
        "max_error_count": max_error_count,
        "destinations": list(destinations),
    }


def rejection_threshold_exceeded(total_rows: int, rejected_rows: int, policy: dict[str, Any]) -> bool:
    if rejected_rows == 0:
        return False
    percent = (rejected_rows / total_rows * 100) if total_rows else 100
    return rejected_rows > int(policy["max_error_count"]) or percent > float(policy["max_error_percent"])


def validate_mapping(product: str, mapping_config: dict[str, Any]) -> dict[str, Any]:
    if mapping_config.get("domain") != DOMAIN_NAME:
        raise PipelineError(f"mapping for {product} must map to domain {DOMAIN_NAME}")

    layout_mapping = mapping_config.get("layout_mapping", {})
    if not layout_mapping:
        raise PipelineError(f"mapping for {product} missing layout_mapping")

    required_fields = tuple(mapping_config.get("domain_required_fields", []))
    if set(required_fields) != set(DOMAIN_REQUIRED_FIELDS):
        raise PipelineError(f"mapping for {product} must require fields: {', '.join(DOMAIN_REQUIRED_FIELDS)}")

    mapped_fields = set(layout_mapping.values())
    missing = [field for field in DOMAIN_REQUIRED_FIELDS if field not in mapped_fields]
    if missing:
        raise PipelineError(f"mapping for {product} missing domain fields: {', '.join(missing)}")

    return {"domain": DOMAIN_NAME, "layout_mapping": layout_mapping, "domain_required_fields": list(DOMAIN_REQUIRED_FIELDS)}
