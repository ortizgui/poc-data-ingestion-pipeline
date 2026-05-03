# Pattern: Rejection Threshold Handling

## Description

Collect invalid rows, write rejection detail to S3, and stop processing when rejection volume exceeds the product policy.

## When to Use

Use this pattern in any processing step that can reject individual records while allowing valid rows to proceed.

## Pattern

Normalize the product policy, collect rejected rows with row context, write rejected detail when needed, compare rejected count and percent to thresholds, and raise `RejectedRowsThresholdError` before writing the next successful stage when thresholds are exceeded.

## Example

```python
def rejection_threshold_exceeded(total_rows: int, rejected_rows: int, policy: dict[str, Any]) -> bool:
    if rejected_rows == 0:
        return False
    percent = (rejected_rows / total_rows * 100) if total_rows else 100
    return rejected_rows > int(policy["max_error_count"]) or percent > float(policy["max_error_percent"])
```

```python
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
```

## Files Using This Pattern

- `aws_local.py` - defines policy normalization and threshold checks.
- `glue_harmonization.py` - rejects rows missing required domain fields.
- `enrichment_batch.py` - rejects rows that fail enrichment.
- `local_sfn_runner.py` - handles threshold failures and publishes file-failed messages.
- `tests/test_pipeline.py` - verifies under-threshold and over-threshold behavior.

## Related

- [Decision: Rejection Policy And Downstream Events](../../decisions/007-rejection-policy-and-downstream-events.md)
- [Feature: Rejected-Record Handling](../../intent/feature-rejected-record-handling.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active
