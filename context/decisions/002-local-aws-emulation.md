# Decision: Local AWS Emulation

## Context

The project must validate an AWS-oriented ingestion architecture locally while preserving the path to real AWS deployment.

## Decision

Use MiniStack on `localhost:4566` and Boto3 clients/resources pointed at that endpoint to emulate supported AWS services. Use local Python scripts for services that are not fully represented as managed jobs in the local environment.

## Rationale

Rationale is documented in ` project-context.md` and README. Local emulation keeps feedback fast and low cost while allowing the code to use AWS service APIs. Scripts fill gaps for Glue jobs, ECS work, EventBridge-to-Step-Functions execution, and local evidence generation.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include running against a real AWS development account, mocking every service in tests only, or using another local AWS emulator.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Event-Driven Ingestion](../intent/feature-event-driven-ingestion.md)
- [Feature: Local Execution And Evidence](../intent/feature-local-execution-and-evidence.md)
- [Decision: Tech Stack](001-tech-stack.md)
- [Pattern: Boto3 Local Clients](../knowledge/patterns/boto3-local-clients.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
