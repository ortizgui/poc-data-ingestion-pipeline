from __future__ import annotations

from typing import Any

from aws_local import DATA_BUCKET, SOURCE_BUCKET, partition_prefix, read_csv_text, s3_read_text, s3_write_text


def run_landing(valid_event: dict[str, Any], aws: Any, run_id: str) -> tuple[str, int]:
    source_key = f"{valid_event['product']}/{valid_event['file_name']}"
    raw_key = (
        f"raw/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
        f"{run_id}/{valid_event['file_name']}"
    )
    text = s3_read_text(aws.s3, SOURCE_BUCKET, source_key)
    s3_write_text(aws.s3, DATA_BUCKET, raw_key, text)
    return raw_key, len(read_csv_text(text))
