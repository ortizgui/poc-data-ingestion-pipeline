# Decision: Local Query Simulation

## Context

The project defines Athena-style operational views but needs local validation without depending on an AWS Athena environment.

## Decision

Use DuckDB to load local S3-emulated JSONL datasets into memory and run analytical queries that mirror the operational Athena model.

## Rationale

Rationale is documented in ` project-context.md` and README. DuckDB provides fast local query feedback, supports JSON ingestion, and avoids requiring Athena during POC development. The approach keeps the real AWS path obvious through separate Glue catalog definitions and Athena SQL views.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include running real Athena queries during tests, using SQLite, or skipping local query validation.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Catalog And Query Layer](../intent/feature-catalog-and-query-layer.md)
- [Feature: Local Execution And Evidence](../intent/feature-local-execution-and-evidence.md)
- [Decision: Tech Stack](001-tech-stack.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
