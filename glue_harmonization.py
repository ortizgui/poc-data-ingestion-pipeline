from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from analytics_writer import emit_ingestion_step
from aws_local import (
    DATA_BUCKET,
    DOMAIN_REQUIRED_FIELDS,
    PipelineError,
    RejectedRowsThresholdError,
    normalize_rejection_policy,
    partition_prefix,
    read_csv_text,
    rejected_key_for,
    rejection_record,
    rejection_threshold_exceeded,
    s3_read_text,
    s3_write_text,
    utc_now,
    validate_mapping,
    write_jsonl,
)


def load_depara_mapping(product: str, mapping_key: str, aws: Any) -> dict[str, Any]:
    mapping_config = json.loads(s3_read_text(aws.s3, DATA_BUCKET, mapping_key))
    return validate_mapping(product, mapping_config)


def run_harmonization(raw_key: str, valid_event: dict[str, Any], aws: Any, run_id: str) -> tuple[str, int, str, str | None, int]:
    started_at = utc_now()
    mapping_key = valid_event["product_config"]["mapping_key"]
    processed_key = (
        f"processed/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
        f"{run_id}/{Path(valid_event['file_name']).stem}.parquet"
    )
    try:
        depara = load_depara_mapping(valid_event["product"], mapping_key, aws)
        rows = read_csv_text(s3_read_text(aws.s3, DATA_BUCKET, raw_key))
        source_fields = set(rows[0].keys()) if rows else set()
        missing_source_fields = [source for source in depara["layout_mapping"] if source not in source_fields]
        if missing_source_fields:
            finished_at = utc_now()
            emit_ingestion_step(
                aws,
                valid_event,
                step_name="HarmonizationGlue",
                step_order=3,
                glue_job_name="glue_harmonization",
                glue_job_run_id=valid_event.get("execution_id", run_id),
                status="FAILED",
                started_at=started_at,
                finished_at=finished_at,
                input_records=0,
                output_records=0,
                error_records=1,
                input_path=f"s3://{DATA_BUCKET}/{raw_key}",
                output_path=f"s3://{DATA_BUCKET}/{processed_key}",
                error_message=f"file missing source columns: {', '.join(missing_source_fields)}",
                error_type="PipelineError",
                error_code="harmonization_schema_failed",
                error_category="pipeline",
                source_bucket=DATA_BUCKET,
                source_key=raw_key,
                payload_ref=f"s3://{DATA_BUCKET}/{raw_key}",
                occurred_at=finished_at,
                schema_name="source_layout_mapping",
                schema_version="v1",
                validation_result="FAILED",
                missing_columns=", ".join(missing_source_fields),
                unexpected_columns="",
                invalid_types="",
                validation_message=f"file missing source columns: {', '.join(missing_source_fields)}",
                validated_at=finished_at,
            )
            raise PipelineError(f"file missing source columns: {', '.join(missing_source_fields)}")

        policy = normalize_rejection_policy(valid_event["product"], valid_event["product_config"])
        domain_rows = []
        rejected_rows = []

        for index, row in enumerate(rows, start=1):
            mapped_row = {
                domain_field: row.get(source_field, "")
                for source_field, domain_field in depara["layout_mapping"].items()
            }
            domain_row = {field: mapped_row.get(field, "") for field in DOMAIN_REQUIRED_FIELDS}
            missing = [field for field in DOMAIN_REQUIRED_FIELDS if not domain_row.get(field)]
            if missing:
                rejected_rows.append(
                    rejection_record(
                        valid_event,
                        run_id,
                        "harmonization",
                        index,
                        f"missing domain fields: {', '.join(missing)}",
                        row,
                    )
                )
                continue
            domain_rows.append(domain_row)

        file_stem = Path(valid_event["file_name"]).stem
        rejection_key = None
        rejection_kwargs: dict[str, Any] = {}
        if rejected_rows:
            rejection_key = rejected_key_for(valid_event, run_id, file_stem, "harmonization")
            s3_write_text(aws.s3, DATA_BUCKET, rejection_key, write_jsonl(rejected_rows))
            rejection_kwargs = {
                "rejection_reason": rejected_rows[0]["reason"],
                "rejection_category": "required_field_validation",
                "rejected_count_summary": len(rejected_rows),
                "total_step_records_summary": len(rows),
                "rejection_percent": (len(rejected_rows) / len(rows) * 100) if rows else 0,
                "rejected_detail_path": f"s3://{DATA_BUCKET}/{rejection_key}",
                "sample_message": rejected_rows[0]["reason"],
            }
            if rejection_threshold_exceeded(len(rows), len(rejected_rows), policy):
                message = (
                    "rejected rows exceeded threshold: "
                    f"{len(rejected_rows)}/{len(rows)} rows; "
                    f"limits {policy['max_error_percent']}% or {policy['max_error_count']} rows; "
                    f"rejected=s3://{DATA_BUCKET}/{rejection_key}"
                )
                raise RejectedRowsThresholdError(message, rejection_key, len(rejected_rows), len(rows))

        s3_write_text(aws.s3, DATA_BUCKET, processed_key, write_jsonl(domain_rows))
        finished_at = utc_now()
        emit_ingestion_step(
            aws,
            valid_event,
            step_name="HarmonizationGlue",
            step_order=3,
            glue_job_name="glue_harmonization",
            glue_job_run_id=valid_event.get("execution_id", run_id),
            status="SUCCEEDED",
            started_at=started_at,
            finished_at=finished_at,
            input_records=len(rows),
            output_records=len(domain_rows),
            rejected_records=len(rejected_rows),
            input_path=f"s3://{DATA_BUCKET}/{raw_key}",
            output_path=f"s3://{DATA_BUCKET}/{processed_key}",
            schema_name="source_layout_mapping",
            schema_version="v1",
            validation_result="SUCCEEDED",
            missing_columns="",
            unexpected_columns=", ".join(sorted(source_fields - set(depara["layout_mapping"].keys()))),
            invalid_types="",
            validation_message="source columns satisfied mapping",
            validated_at=finished_at,
            rule_name="required_domain_fields",
            rule_type="completeness",
            rule_result="FAILED" if rejected_rows else "SUCCEEDED",
            valid_records=len(domain_rows),
            invalid_records=len(rejected_rows),
            warning_records=0,
            threshold_value=str(policy["max_error_percent"]),
            measured_value=str((len(rejected_rows) / len(rows) * 100) if rows else 0),
            quality_details=f"mapping={mapping_key}",
            measured_at=finished_at,
            artifact_type="processed",
            artifact_role="harmonization_output",
            lineage_bucket=DATA_BUCKET,
            lineage_key=processed_key,
            lineage_format="parquet",
            record_count_lineage=len(domain_rows),
            file_size_bytes=len(write_jsonl(domain_rows).encode("utf-8")),
            parent_bucket=DATA_BUCKET,
            parent_key=raw_key,
            lineage_created_at=finished_at,
            **rejection_kwargs,
        )
        return processed_key, len(domain_rows), mapping_key, rejection_key, len(rejected_rows)
    except Exception as exc:
        finished_at = utc_now()
        emit_ingestion_step(
            aws,
            valid_event,
            step_name="HarmonizationGlue",
            step_order=3,
            glue_job_name="glue_harmonization",
            glue_job_run_id=valid_event.get("execution_id", run_id),
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            input_records=0,
            output_records=0,
            error_records=1,
            input_path=f"s3://{DATA_BUCKET}/{raw_key}",
            output_path=f"s3://{DATA_BUCKET}/{processed_key}",
            error_message=str(exc),
            error_type=exc.__class__.__name__,
            error_code="harmonization_failed",
            error_category="pipeline",
            source_bucket=DATA_BUCKET,
            source_key=raw_key,
            payload_ref=f"s3://{DATA_BUCKET}/{raw_key}",
            occurred_at=finished_at,
        )
        raise
