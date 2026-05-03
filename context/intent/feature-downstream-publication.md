# Feature: Downstream Event Publication

## What

The pipeline publishes events for curated business records, rejected records, and file-level failures to downstream destinations.

## Why

This lets consuming systems receive accepted business data and quality events without reading the data lake directly, while keeping rejection and failure communication consistent.

## Acceptance Criteria

- [ ] Curated records produce downstream business events.
- [ ] Rejected records produce downstream quality events.
- [ ] File-level failures produce downstream failure events.
- [ ] Destination routing follows product configuration.

## Related

- [Project Intent](project-intent.md)
- [Decision: Rejection Policy And Downstream Events](../decisions/007-rejection-policy-and-downstream-events.md)
- [Pattern: Downstream Message Publishing](../knowledge/patterns/downstream-message-publishing.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
