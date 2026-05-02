from __future__ import annotations

import re
from typing import Any

from aws_local import ANALYTICS_BUCKET, anomesdia_for, s3_write_text, utc_now, write_jsonl


DATASET_PREFIXES = {
    "analytics_ingestion_runs": "observability/ingestion_runs",
    "analytics_ingestion_steps": "observability/ingestion_steps",
    "analytics_ingestion_errors": "observability/ingestion_errors",
    "analytics_ingestion_rejections_summary": "observability/ingestion_rejections",
    "analytics_data_quality_summary": "quality/data_quality_summary",
    "analytics_schema_validation": "quality/schema_validation",
    "analytics_file_lineage": "audit/file_lineage",
    "analytics_execution_events": "audit/execution_events",
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


def write_error_event(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"error-{record.get('step_name', 'unknown')}-{record.get('error_type', 'error')}"
    return write_dataset_record(aws, "analytics_ingestion_errors", record, suffix)


def write_rejection_summary(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"rejection-{record.get('step_name', 'unknown')}-{record.get('rejection_category', 'summary')}"
    return write_dataset_record(aws, "analytics_ingestion_rejections_summary", record, suffix)


def write_data_quality_summary(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"quality-{record.get('step_name', 'unknown')}-{record.get('rule_name', 'rule')}"
    return write_dataset_record(aws, "analytics_data_quality_summary", record, suffix)


def write_schema_validation(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"schema-{record.get('step_name', 'unknown')}-{record.get('validation_result', 'result')}"
    return write_dataset_record(aws, "analytics_schema_validation", record, suffix)


def write_file_lineage_event(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"lineage-{record.get('artifact_type', 'artifact')}-{record.get('artifact_role', 'role')}"
    return write_dataset_record(aws, "analytics_file_lineage", record, suffix)


def write_execution_event(aws: Any, record: dict[str, Any]) -> str:
    suffix = f"event-{record.get('step_name', 'pipeline')}-{record.get('event_type', 'event')}"
    return write_dataset_record(aws, "analytics_execution_events", record, suffix)


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
    }
    return write_ingestion_step(aws, {**analytics_dimensions(event_context), **defaults, **fields})


def emit_error_event(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    defaults = {"is_retryable": False}
    return write_error_event(aws, {**analytics_dimensions(event_context), **defaults, **fields})


def emit_rejection_summary(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    return write_rejection_summary(aws, {**analytics_dimensions(event_context), **fields})


def emit_data_quality_summary(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    return write_data_quality_summary(aws, {**analytics_dimensions(event_context), **fields})


def emit_schema_validation(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    return write_schema_validation(aws, {**analytics_dimensions(event_context), **fields})


def emit_file_lineage_event(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    return write_file_lineage_event(aws, {**analytics_dimensions(event_context), **fields})


def emit_execution_event(aws: Any, event_context: dict[str, Any], **fields: Any) -> str:
    return write_execution_event(aws, {**analytics_dimensions(event_context), **fields})
