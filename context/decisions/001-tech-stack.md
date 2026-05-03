# Decision: Tech Stack

## Context

The project is a local proof of concept for a generic AWS-style data ingestion pipeline. Existing documentation emphasizes simple architecture, low cost, local execution, traceability, and low product customization.

## Decision

Use Python scripts as the implementation language, Boto3 and Botocore for AWS service access, MiniStack for local AWS emulation, DuckDB for local Athena-like analytics queries, Docker Compose to run MiniStack, JSON configuration files for product mappings, and Python `unittest` for tests.

## Rationale

Rationale is inferred from the implementation and README. Python keeps local pipeline jobs simple and scriptable. Boto3 mirrors real AWS service APIs. MiniStack supports the target AWS services locally. DuckDB provides fast local analytical validation without requiring Athena. JSON configuration keeps product onboarding outside code. `unittest` avoids extra test dependencies.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include LocalStack instead of MiniStack, Pytest instead of `unittest`, real Athena instead of DuckDB for local validation, and a packaged application framework instead of standalone scripts.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Local Execution And Evidence](../intent/feature-local-execution-and-evidence.md)
- [Decision: Local AWS Emulation](002-local-aws-emulation.md)
- [Decision: Local Query Simulation](008-local-query-simulation.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
