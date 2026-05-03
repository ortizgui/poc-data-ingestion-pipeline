# Pattern: S3 Partitioned Keys

## Description

Build S3 keys with stage, product, business-date partitions, run ID, and file name so outputs are traceable and query-friendly.

## When to Use

Use this pattern for business lake files, rejected records, manifests, and analytics records that need stable run-level traceability.

## Pattern

Use `year=YYYY/month=MM/day=DD` for business lake stages and `anomesdia=YYYYMMDD` for analytics datasets.

## Example

```python
def partition_prefix(business_date: str) -> str:
    date = parse_business_date(business_date)
    return f"year={date.year:04d}/month={date.month:02d}/day={date.day:02d}"
```

```python
raw_key = (
    f"raw/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
    f"{run_id}/{valid_event['file_name']}"
)
```

```python
key = f"{prefix}/anomesdia={anomesdia}/{execution_id}-{sanitize_fragment(suffix)}.parquet"
```

## Files Using This Pattern

- `aws_local.py` - defines date partition helpers.
- `glue_landing.py` - writes raw files with product/date/run partitions.
- `glue_harmonization.py` - writes processed and rejected files with partitioned keys.
- `enrichment_batch.py` - writes curated and rejected files with partitioned keys.
- `analytics_writer.py` - writes analytics records partitioned by `anomesdia`.

## Related

- [Decision: S3 Data Lake Layout](../../decisions/005-s3-data-lake-layout.md)
- [Feature: Business Data Lake Processing](../../intent/feature-business-data-lake-processing.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active
