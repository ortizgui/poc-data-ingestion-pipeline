# Feature: Catalog And Query Layer

## What

The project exposes business and operational datasets through catalog definitions and queryable operational views.

## Why

This allows analytics, dashboarding, troubleshooting, and audit workflows to query standardized tables and views instead of manually inspecting pipeline files.

## Acceptance Criteria

- [ ] Business and analytics datasets have catalog definitions.
- [ ] Operational views summarize run status, quality, schema, errors, rejections, and lineage.
- [ ] Curated and rejected business records can be joined with run context.
- [ ] Local execution can validate the query model.

## Related

- [Project Intent](project-intent.md)
- [Decision: Local Query Simulation](../decisions/008-local-query-simulation.md)
- [Decision: Enriched Analytics Model](../decisions/006-enriched-analytics-model.md)
- [Pattern: Analytics Step Emission](../knowledge/patterns/analytics-step-emission.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
