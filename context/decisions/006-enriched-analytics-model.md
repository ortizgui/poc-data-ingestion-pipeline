# Decision: Enriched Analytics Model

## Context

The project evolved from business-only ingestion to business plus operational analytics. It needs observability, audit, quality, schema, rejection, error, and lineage data while keeping the model simple.

## Decision

Use two analytics datasets: run-level records and enriched step-level records. Store error, quality, schema, rejection, and lineage fields inside the step dataset instead of creating many separate analytics tables.

## Rationale

Rationale is documented in ` project-context.md`. A two-table model keeps analytics rules centralized, reduces table sprawl, simplifies local and Athena queries, and lets one step row tell the full operational story for that attempt.

## Alternatives Considered

Alternatives are partially documented in ` project-context.md` as avoided complexity. Plausible alternatives include separate tables for errors, quality, schema validation, rejections, lineage, and events, or writing only final run summaries.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Operational Analytics And Audit](../intent/feature-operational-analytics.md)
- [Feature: Catalog And Query Layer](../intent/feature-catalog-and-query-layer.md)
- [Pattern: Analytics Step Emission](../knowledge/patterns/analytics-step-emission.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
