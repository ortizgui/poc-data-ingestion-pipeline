# Pattern: Analytics Step Emission

## Description

Write operational analytics through shared emit helpers so run-level and step-level facts use consistent dimensions and S3 paths.

## When to Use

Use this pattern whenever a pipeline step starts, succeeds, fails, records quality results, records schema validation, records rejection summaries, or records lineage.

## Pattern

Call `emit_ingestion_step` or `emit_ingestion_run` with local step fields while shared dimensions and output paths stay centralized.

## Example

```python
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
```

```python
emit_ingestion_step(
    aws,
    valid_event,
    step_name="HarmonizationGlue",
    step_order=3,
    status="SUCCEEDED",
    input_records=len(rows),
    output_records=len(domain_rows),
    rejected_records=len(rejected_rows),
    rule_name="required_domain_fields",
    schema_name="source_layout_mapping",
    artifact_type="processed",
    lineage_key=processed_key,
)
```

## Files Using This Pattern

- `analytics_writer.py` - owns common dimensions, dataset paths, and emit helpers.
- `glue_landing.py` - emits landing success and failure telemetry.
- `glue_harmonization.py` - emits schema, quality, rejection, lineage, and error telemetry.
- `enrichment_batch.py` - emits quality, rejection, lineage, and error telemetry.
- `local_sfn_runner.py` - emits initialization and final run analytics.

## Related

- [Decision: Enriched Analytics Model](../../decisions/006-enriched-analytics-model.md)
- [Decision: S3 Data Lake Layout](../../decisions/005-s3-data-lake-layout.md)
- [Feature: Operational Analytics And Audit](../../intent/feature-operational-analytics.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active
