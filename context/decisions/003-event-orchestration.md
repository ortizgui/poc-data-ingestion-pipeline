# Decision: Event Orchestration

## Context

The project needs a generic ingestion flow where file-ready events start a predictable sequence of validation, landing, harmonization, enrichment, notification, and finalization steps.

## Decision

Use an EventBridge-style file-ready event to trigger a Step Functions-style state machine. The local implementation stores the state machine in ASL JSON and dispatches each state resource to local Python functions.

## Rationale

Rationale is inferred from the README, `state-machine.asl.json`, and `local_sfn_runner.py`. The state machine keeps step order explicit, avoids hardcoding the flow inside the event publisher, and mirrors the intended AWS architecture while remaining executable locally.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include direct function calls from `pipeline.py`, a custom workflow engine, or a queue-only flow without explicit orchestration.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Event-Driven Ingestion](../intent/feature-event-driven-ingestion.md)
- [Decision: Local AWS Emulation](002-local-aws-emulation.md)
- [Pattern: ASL Resource Dispatch](../knowledge/patterns/asl-resource-dispatch.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
