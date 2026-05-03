# Feature: Config-Driven Product Onboarding

## What

The project supports multiple products by storing product-specific setup outside the pipeline's core processing logic.

## Why

This keeps the ingestion pipeline generic, reduces code changes for new products, and lets different product file layouts flow into the same business domain.

## Acceptance Criteria

- [ ] Configured products can be processed without product-specific code paths.
- [ ] Different source layouts can map to the same target domain.
- [ ] Products without valid configuration are rejected before processing.
- [ ] Product destinations and quality policies are configurable.

## Related

- [Project Intent](project-intent.md)
- [Decision: Config Driven Product Mapping](../decisions/004-config-driven-product-mapping.md)
- [Pattern: Product Mapping Validation](../knowledge/patterns/product-mapping-validation.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
