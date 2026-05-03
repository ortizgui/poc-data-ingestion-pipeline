# Feature: Event-Driven Ingestion

## What

The pipeline accepts product file-ready notifications and starts an ingestion run for each configured product file.

## Why

This allows product teams to trigger ingestion when data is available, keeps the pipeline responsive to source activity, and supports processing multiple product files through a shared flow.

## Acceptance Criteria

- [ ] A product can publish a file-ready event.
- [ ] Each valid event starts one ingestion run.
- [ ] Missing or unconfigured products are skipped with clear failure evidence.
- [ ] Each run keeps a stable ingestion identity for traceability.

## Related

- [Project Intent](project-intent.md)
- [Decision: Event Orchestration](../decisions/003-event-orchestration.md)
- [Decision: Local AWS Emulation](../decisions/002-local-aws-emulation.md)
- [Pattern: ASL Resource Dispatch](../knowledge/patterns/asl-resource-dispatch.md)
- [Pattern: Boto3 Local Clients](../knowledge/patterns/boto3-local-clients.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
