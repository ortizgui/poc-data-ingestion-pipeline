# Decision: Rejection Policy And Downstream Events

## Context

The pipeline must preserve valid records, capture invalid records, fail runs that exceed tolerance, and notify downstream consumers about accepted and rejected data.

## Decision

Use product-level rejection policies with maximum error percent and count thresholds. Write rejected detail to S3, fail the run when thresholds are exceeded, and publish curated, rejected, and file-failed events through a queue-to-worker-to-topic flow.

## Rationale

Rationale is documented in ` project-context.md` and README. S3 is a better fit than DynamoDB for potentially large rejected detail. Product-level thresholds let products define tolerable data quality. A common downstream path keeps accepted records and quality events consistent.

## Alternatives Considered

Alternatives not documented in existing codebase. Plausible alternatives include failing any file with a single rejected row, writing all rejects to DynamoDB, separate workers for curated and rejected files, or requiring consumers to poll S3 directly.

## Outcomes

Outcomes to be documented as project evolves.

## Related

- [Project Intent](../intent/project-intent.md)
- [Feature: Rejected-Record Handling](../intent/feature-rejected-record-handling.md)
- [Feature: Downstream Event Publication](../intent/feature-downstream-publication.md)
- [Pattern: Rejection Threshold Handling](../knowledge/patterns/rejection-threshold-handling.md)
- [Pattern: Downstream Message Publishing](../knowledge/patterns/downstream-message-publishing.md)

## Status

- **Created**: 2026-05-02 (Phase: Intent)
- **Status**: Accepted
- **Note**: Documented from existing implementation
