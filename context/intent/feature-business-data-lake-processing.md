# Feature: Business Data Lake Processing

## What

The pipeline moves valid product data through raw, processed, and curated stages, preserving the source copy and producing a shared enriched business output.

## Why

This provides traceable, staged data processing so business consumers can rely on standardized curated records while operations can inspect earlier stages when needed.

## Acceptance Criteria

- [ ] Source files are copied to a raw data zone.
- [ ] Product-specific layouts are transformed into the shared business domain.
- [ ] Valid records are enriched and written to the curated zone.
- [ ] Processing outputs keep business-date partitioning and run traceability.

## Related

- [Project Intent](project-intent.md)
- [Decision: S3 Data Lake Layout](../decisions/005-s3-data-lake-layout.md)
- [Decision: Config Driven Product Mapping](../decisions/004-config-driven-product-mapping.md)
- [Pattern: S3 Partitioned Keys](../knowledge/patterns/s3-partitioned-keys.md)
- [Pattern: Product Mapping Validation](../knowledge/patterns/product-mapping-validation.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Active (already implemented)
