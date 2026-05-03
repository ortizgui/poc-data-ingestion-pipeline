# Feature: Rejected-Record Handling

## What

The pipeline separates invalid records from valid records and can stop a run when rejected rows exceed the configured product tolerance.

## Why

This preserves good data when quality issues are limited, prevents low-quality files from advancing, and gives teams detailed evidence for correction and monitoring.

## Acceptance Criteria

- [ ] Invalid rows are recorded with enough context to understand the rejection.
- [ ] Valid rows continue when rejection volume is within tolerance.
- [ ] Runs fail when rejection volume exceeds tolerance.
- [ ] Rejected records and failed files can continue to downstream quality handling.

## Related

- [Project Intent](project-intent.md)
- [Decision: Rejection Policy And Downstream Events](../decisions/007-rejection-policy-and-downstream-events.md)
- [Pattern: Rejection Threshold Handling](../knowledge/patterns/rejection-threshold-handling.md)
- [Pattern: Downstream Message Publishing](../knowledge/patterns/downstream-message-publishing.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
