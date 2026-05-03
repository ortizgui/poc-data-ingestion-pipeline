# Decision: S3 Data Lake Layout

## Context

The pipeline needs traceable business data stages and operational datasets that can be queried by date and joined with run context.

## Decision

Use S3 buckets for source, business lake, and analytics lake data. Business outputs are organized into `raw`, `processed`, `curated`, and `rejected` zones partitioned by business date. Analytics outputs are organized under observability prefixes partitioned by `anomesdia`.

## Rationale

Rationale is documented in ` project-context.md`. S3 is low cost for historical files and high-volume rejected detail. Date partitioning supports time-based queries. Separate business and analytics locations reduce coupling and keep operational analytics distinct from business datasets.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include storing rejected detail in DynamoDB, mixing analytics with business data in one prefix, or using additional partitions such as product and status from the start.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Business Data Lake Processing](../intent/feature-business-data-lake-processing.md)
- [Feature: Operational Analytics And Audit](../intent/feature-operational-analytics.md)
- [Pattern: S3 Partitioned Keys](../knowledge/patterns/s3-partitioned-keys.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
