from __future__ import annotations

import re
from typing import Any

from aws_local import ANALYTICS_BUCKET, anomesdia_for, s3_write_text, utc_now, write_jsonl


DATASET_PREFIXES = {
    "analytics_ingestion_runs": "observability/ingestion_runs",
    "analytics_ingestion_steps": "observability/ingestion_steps",
}


def sanitize_fragment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "-", value).strip("-") or "item"


def write_dataset_record(aws: Any, dataset: str, record: dict[str, Any], suffix: str) -> str:
    prefix = DATASET_PREFIXES[dataset]
    anomesdia = record["anomesdia"]
    execution_id = sanitize_fragment(record.get("execution_id") or record.get("ingestion_id") or record.get("correlation_id") or utc_now())
    key = f"{prefix}/anomesdia={anomesdia}/{execution_id}-{sanitize_fragment(suffix)}.parquet"
    s3_write_text(aws.s3, ANALYTICS_BUCKET, key, write_jsonl([record]))
    return f"s3://{ANALYTICS_BUCKET}/{key}"


def write_ingestion_run(aws: Any, record: dict[str, Any]) -> str:
    return write_dataset_record(aws, "analytics_ingestion_runs", record, "run")


def write_ingestion_step(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"step-{record.get('step_name', 'unknown')}-{record.get('status', 'unknown')}"
    return write_dataset_record(aws, "analytics_ingestion_steps", record, suffix)


def analytics_dimensions(event_context: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    ingestion_id = event_context.get("ingestion_id") or event_context.get("run_id") or run_id or "unknown"
    execution_id = event_context.get("execution_id") or event_context.get("run_id") or run_id or "unknown"
    return {
        "ingestion_id": ingestion_id,
        "execution_id": execution_id,
        "correlation_id": event_context.get("correlation_id") or ingestion_id,
        "product": event_context.get("product", "unknown"),
        "domain": event_context.get("domain") or event_context.get("product_config", {}).get("domain") or "unknown",
        "anomesdia": event_context.get("anomesdia") or anomesdia_for(event_context["business_date"]),
    }


def emit_ingestion_run(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    return write_ingestion_run(aws, {**analytics_dimensions(event_context), **fields})


def emit_ingestion_step(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    defaults = {
        "attempt": 1,
        "duration_seconds": 0,
        "rejected_records": 0,
        "error_records": 0,
        "error_message": "",
        "input_records": 0,
        "output_records": 0,
        "input_path": "",
        "output_path": "",
    }
    return write_ingestion_step(aws, {**analytics_dimensions(event_context), **defaults, **fields})
