from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from analytics_writer import (
    emit_data_quality_summary,
    emit_error_event,
    emit_execution_event,
    emit_file_lineage_event,
    emit_ingestion_step,
    emit_rejection_summary,
)
from aws_local import (
    DATA_BUCKET,
    DOMAIN_NAME,
    RejectedRowsThresholdError,
    normalize_rejection_policy,
    partition_prefix,
    rejection_threshold_exceeded,
    s3_read_text,
    s3_write_text,
    utc_now,
    write_jsonl,
)


def rejected_key_for(valid_event: dict[str, Any], run_id: str, file_stem: str, stage: str) -> str:
    return (
        f"rejected/{stage}/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
        f"{run_id}/{file_stem}.jsonl"
    )


def rejection_record(
    valid_event: dict[str, Any],
    run_id: str,
    stage: str,
    row_number: int,
    reason: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "stage": stage,
        "product": valid_event["product"],
        "business_date": valid_event["business_date"],
        "file_name": valid_event["file_name"],
        "row_number": row_number,
        "reason": reason,
        "raw_row": row,
    }


def enrich_row(row: dict[str, Any], valid_event: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "domain": DOMAIN_NAME,
        "product": valid_event["product"],
        "business_date": valid_event["business_date"],
        "enriched_at": utc_now(),
    }


def run_enrichment(processed_key: str, valid_event: dict[str, Any], aws: Any) -> tuple[str, int, str | None, int]:
    started_at = utc_now()
    try:
        rows = [json.loads(line) for line in s3_read_text(aws.s3, DATA_BUCKET, processed_key).splitlines() if line.strip()]
        policy = normalize_rejection_policy(valid_event["product"], valid_event["product_config"])
        enriched_rows = []
        rejected_rows = []

        for index, row in enumerate(rows, start=1):
            try:
                enriched_rows.append(enrich_row(row, valid_event))
            except Exception as exc:
                rejected_rows.append(rejection_record(valid_event, valid_event["run_id"], "enrichment", index, str(exc), row))

        file_stem = Path(valid_event["file_name"]).stem
        rejection_key = None
        if rejected_rows:
            rejection_key = rejected_key_for(valid_event, valid_event["run_id"], file_stem, "enrichment")
            s3_write_text(aws.s3, DATA_BUCKET, rejection_key, write_jsonl(rejected_rows))
            emit_rejection_summary(
                aws,
                valid_event,
                step_name="EnrichmentBatch",
                rejection_reason=rejected_rows[0]["reason"],
                rejection_category="enrichment_lookup",
                rejected_count=len(rejected_rows),
                total_step_records=len(rows),
                rejection_percent=(len(rejected_rows) / len(rows) * 100) if rows else 0,
                rejected_detail_path=f"s3://{DATA_BUCKET}/{rejection_key}",
                sample_message=rejected_rows[0]["reason"],
                occurred_at=utc_now(),
            )
            if rejection_threshold_exceeded(len(rows), len(rejected_rows), policy):
                message = (
                    "rejected rows exceeded threshold: "
                    f"{len(rejected_rows)}/{len(rows)} rows; "
                    f"limits {policy['max_error_percent']}% or {policy['max_error_count']} rows; "
                    f"rejected=s3://{DATA_BUCKET}/{rejection_key}"
                )
                raise RejectedRowsThresholdError(message, rejection_key, len(rejected_rows), len(rows))

        curated_key = processed_key.replace("processed/", "curated/", 1)
        s3_write_text(aws.s3, DATA_BUCKET, curated_key, write_jsonl(enriched_rows))
        finished_at = utc_now()
        emit_data_quality_summary(
            aws,
            valid_event,
            step_name="EnrichmentBatch",
            rule_name="enrichment_success_rate",
            rule_type="enrichment",
            rule_result="FAILED" if rejected_rows else "SUCCEEDED",
            total_records=len(rows),
            valid_records=len(enriched_rows),
            invalid_records=len(rejected_rows),
            warning_records=0,
            threshold_value=str(policy["max_error_percent"]),
            measured_value=str((len(rejected_rows) / len(rows) * 100) if rows else 0),
            details="enrichment batch summary",
            measured_at=finished_at,
        )
        emit_ingestion_step(
            aws,
            valid_event,
            step_name="EnrichmentBatch",
            step_order=4,
            glue_job_name="enrichment_batch",
            glue_job_run_id=valid_event.get("execution_id", valid_event.get("run_id", "unknown")),
            status="SUCCEEDED",
            started_at=started_at,
            finished_at=finished_at,
            input_records=len(rows),
            output_records=len(enriched_rows),
            rejected_records=len(rejected_rows),
            input_path=f"s3://{DATA_BUCKET}/{processed_key}",
            output_path=f"s3://{DATA_BUCKET}/{curated_key}",
        )
        emit_file_lineage_event(
            aws,
            valid_event,
            artifact_type="curated",
            artifact_role="enrichment_output",
            bucket=DATA_BUCKET,
            s3_key=curated_key,
            format="parquet",
            record_count=len(enriched_rows),
            file_size_bytes=len(write_jsonl(enriched_rows).encode("utf-8")),
            parent_bucket=DATA_BUCKET,
            parent_key=processed_key,
            created_at=finished_at,
        )
        emit_execution_event(
            aws,
            valid_event,
            step_name="EnrichmentBatch",
            event_type="step_completed",
            event_source="enrichment_batch",
            event_message=f"enriched={len(enriched_rows)} rejected={len(rejected_rows)}",
            event_payload_ref=f"s3://{DATA_BUCKET}/{curated_key}",
            event_at=finished_at,
        )
        return curated_key, len(enriched_rows), rejection_key, len(rejected_rows)
    except Exception as exc:
        finished_at = utc_now()
        output_key = processed_key.replace("processed/", "curated/", 1)
        emit_ingestion_step(
            aws,
            valid_event,
            step_name="EnrichmentBatch",
            step_order=4,
            glue_job_name="enrichment_batch",
            glue_job_run_id=valid_event.get("execution_id", valid_event.get("run_id", "unknown")),
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            input_records=0,
            output_records=0,
            error_records=1,
            input_path=f"s3://{DATA_BUCKET}/{processed_key}",
            output_path=f"s3://{DATA_BUCKET}/{output_key}",
            error_message=str(exc),
        )
        emit_error_event(
            aws,
            valid_event,
            step_name="EnrichmentBatch",
            error_type=exc.__class__.__name__,
            error_code="enrichment_failed",
            error_message=str(exc),
            error_category="pipeline",
            glue_job_name="enrichment_batch",
            glue_job_run_id=valid_event.get("execution_id", valid_event.get("run_id", "unknown")),
            source_bucket=DATA_BUCKET,
            source_key=processed_key,
            payload_ref=f"s3://{DATA_BUCKET}/{processed_key}",
            occurred_at=finished_at,
        )
        emit_execution_event(
            aws,
            valid_event,
            step_name="EnrichmentBatch",
            event_type="step_failed",
            event_source="enrichment_batch",
            event_message=str(exc),
            event_payload_ref=f"s3://{DATA_BUCKET}/{processed_key}",
            event_at=finished_at,
        )
        raise
