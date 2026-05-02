from __future__ import annotations

from typing import Any

from analytics_writer import emit_ingestion_step
from aws_local import DATA_BUCKET, SOURCE_BUCKET, partition_prefix, read_csv_text, s3_read_text, s3_write_text, utc_now


def run_landing(valid_event: dict[str, Any], aws: Any, run_id: str) -> tuple[str, int]:
    started_at = utc_now()
    source_key = f"{valid_event['product']}/{valid_event['file_name']}"
    raw_key = (
        f"raw/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
        f"{run_id}/{valid_event['file_name']}"
    )
    try:
        text = s3_read_text(aws.s3, SOURCE_BUCKET, source_key)
        s3_write_text(aws.s3, DATA_BUCKET, raw_key, text)
        raw_rows = len(read_csv_text(text))
        finished_at = utc_now()
        emit_ingestion_step(
            aws,
            valid_event,
            step_name="LandingGlue",
            step_order=2,
            glue_job_name="glue_landing",
            glue_job_run_id=valid_event.get("execution_id", run_id),
            status="SUCCEEDED",
            started_at=started_at,
            finished_at=finished_at,
            input_records=raw_rows,
            output_records=raw_rows,
            input_path=f"s3://{SOURCE_BUCKET}/{source_key}",
            output_path=f"s3://{DATA_BUCKET}/{raw_key}",
            artifact_type="raw",
            artifact_role="landing_output",
            lineage_bucket=DATA_BUCKET,
            lineage_key=raw_key,
            lineage_format="csv",
            record_count_lineage=raw_rows,
            file_size_bytes=len(text.encode("utf-8")),
            parent_bucket=SOURCE_BUCKET,
            parent_key=source_key,
            lineage_created_at=finished_at,
        )
        return raw_key, raw_rows
    except Exception as exc:
        finished_at = utc_now()
        emit_ingestion_step(
            aws,
            valid_event,
            step_name="LandingGlue",
            step_order=2,
            glue_job_name="glue_landing",
            glue_job_run_id=valid_event.get("execution_id", run_id),
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            input_records=0,
            output_records=0,
            error_records=1,
            input_path=f"s3://{SOURCE_BUCKET}/{source_key}",
            output_path=f"s3://{DATA_BUCKET}/{raw_key}",
            error_message=str(exc),
            error_type=exc.__class__.__name__,
            error_code="landing_failed",
            error_category="pipeline",
            source_bucket=SOURCE_BUCKET,
            source_key=source_key,
            payload_ref=f"s3://{SOURCE_BUCKET}/{source_key}",
            occurred_at=finished_at,
        )
        raise
