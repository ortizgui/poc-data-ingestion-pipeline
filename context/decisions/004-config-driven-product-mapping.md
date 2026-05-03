# Decision: Config Driven Product Mapping

## Context

The project must ingest multiple product file layouts while keeping the pipeline generic and minimizing code changes for new products.

## Decision

Store product configuration in DynamoDB and store per-product source-to-domain mapping JSON files in S3. Validate each product against the shared `transaction` domain before processing.

## Rationale

Rationale is documented in README and inferred from `config/products.json`, `config/mappings/*.json`, and validation code. Product configuration controls mapping, publish destinations, and rejection policies. Mapping files allow different source column names to feed one shared business domain without product-specific pipeline branches.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include hardcoded product mappings, one code module per product, database-stored mapping definitions only, or a separate schema registry.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Config-Driven Product Onboarding](../intent/feature-product-configuration.md)
- [Feature: Business Data Lake Processing](../intent/feature-business-data-lake-processing.md)
- [Pattern: Product Mapping Validation](../knowledge/patterns/product-mapping-validation.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
