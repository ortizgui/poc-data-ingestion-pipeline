from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aws_local import (
    DATA_BUCKET,
    DOMAIN_REQUIRED_FIELDS,
    PipelineError,
    RejectedRowsThresholdError,
    normalize_rejection_policy,
    partition_prefix,
    read_csv_text,
    rejection_threshold_exceeded,
    s3_read_text,
    s3_write_text,
    validate_mapping,
    write_jsonl,
)


def load_depara_mapping(product: str, mapping_key: str, aws: Any) -> dict[str, Any]:
    mapping_config = json.loads(s3_read_text(aws.s3, DATA_BUCKET, mapping_key))
    return validate_mapping(product, mapping_config)


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


def run_harmonization(raw_key: str, valid_event: dict[str, Any], aws: Any, run_id: str) -> tuple[str, int, str, str | None, int]:
    mapping_key = valid_event["product_config"]["mapping_key"]
    depara = load_depara_mapping(valid_event["product"], mapping_key, aws)
    rows = read_csv_text(s3_read_text(aws.s3, DATA_BUCKET, raw_key))
    source_fields = set(rows[0].keys()) if rows else set()
    missing_source_fields = [source for source in depara["layout_mapping"] if source not in source_fields]
    if missing_source_fields:
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
    if rejected_rows:
        rejection_key = rejected_key_for(valid_event, run_id, file_stem, "harmonization")
        s3_write_text(aws.s3, DATA_BUCKET, rejection_key, write_jsonl(rejected_rows))
        if rejection_threshold_exceeded(len(rows), len(rejected_rows), policy):
            message = (
                "rejected rows exceeded threshold: "
                f"{len(rejected_rows)}/{len(rows)} rows; "
                f"limits {policy['max_error_percent']}% or {policy['max_error_count']} rows; "
                f"rejected=s3://{DATA_BUCKET}/{rejection_key}"
            )
            raise RejectedRowsThresholdError(message, rejection_key, len(rejected_rows), len(rows))

    processed_key = (
        f"processed/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
        f"{run_id}/{file_stem}.parquet"
    )
    s3_write_text(aws.s3, DATA_BUCKET, processed_key, write_jsonl(domain_rows))
    return processed_key, len(domain_rows), mapping_key, rejection_key, len(rejected_rows)
