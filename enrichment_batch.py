from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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
    return curated_key, len(enriched_rows), rejection_key, len(rejected_rows)
