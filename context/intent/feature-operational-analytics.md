# Feature: Operational Analytics And Audit

## What

Each ingestion run produces operational facts that describe run status, processing steps, record counts, errors, quality results, schema checks, rejections, and lineage.

## Why

This gives support, operations, and business stakeholders visibility into pipeline health, data quality, failure causes, and source-to-output traceability.

## Acceptance Criteria

- [ ] Each run records a final status and run-level summary.
- [ ] Each processing step records telemetry.
- [ ] Errors, quality checks, schema checks, rejection summaries, and lineage are available for analysis.
- [ ] Failed runs still produce analytics evidence.

## Related

- [Project Intent](project-intent.md)
- [Decision: Enriched Analytics Model](../decisions/006-enriched-analytics-model.md)
- [Pattern: Analytics Step Emission](../knowledge/patterns/analytics-step-emission.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
