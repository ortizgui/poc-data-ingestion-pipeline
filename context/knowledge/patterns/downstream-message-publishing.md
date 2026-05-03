# Pattern: Downstream Message Publishing

## Description

Read S3 notification or pipeline failure messages from a queue, load the related file when needed, and publish normalized downstream events to configured destinations.

## When to Use

Use this pattern for local worker logic that converts curated records, rejected records, or file failures into consumer-facing events.

## Pattern

Normalize queue messages into bucket/key records, derive product and run context from the message or key, fetch product destinations, ensure a topic and destination queue exist, and publish one event per row or one file-failed event.

## Example

```python
def process_message(message: dict[str, Any], sns: Any, sqs: Any, s3: Any) -> list[str]:
    published = []
    for record in s3_records(message):
        if record["key"].startswith("rejected/") or message.get("event_type") in {
            "ingestion.records-rejected",
            "ingestion.file-failed",
        }:
            published.extend(publish_rejected_file(message, record, sns, sqs, s3))
        else:
            published.extend(publish_curated_file(record, sns, sqs, s3))
    return published
```

```python
sns.publish(
    TopicArn=topic_arn,
    Message=json.dumps(
        {
            "event_type": "domain_record_ready",
            "destination": destination,
            "run_id": run_id_from_file_key(record["key"]),
            "product": product,
            "record": row,
            "source": {"bucket": record["bucket"], "key": record["key"]},
        },
        sort_keys=True,
    ),
)
```

## Files Using This Pattern

- `ecs_worker.py` - consumes queue messages and publishes curated, rejected, and file-failed events.
- `local_sfn_runner.py` - sends file-failed messages to the worker queue.
- `tests/test_pipeline.py` - verifies rejected-record publication.

## Related

- [Decision: Rejection Policy And Downstream Events](../../decisions/007-rejection-policy-and-downstream-events.md)
- [Feature: Downstream Event Publication](../../intent/feature-downstream-publication.md)
- [Feature: Rejected-Record Handling](../../intent/feature-rejected-record-handling.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active
